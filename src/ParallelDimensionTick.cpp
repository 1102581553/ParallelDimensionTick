#include "ParallelDimensionTick.h"

#include <ll/api/memory/Hook.h>
#include <ll/api/mod/RegisterHelper.h>
#include <ll/api/io/LoggerRegistry.h>

#include <mc/world/level/Level.h>
#include <mc/server/ServerLevel.h>
#include <mc/world/level/dimension/Dimension.h>
#include <mc/world/level/Tick.h>
#include <mc/world/level/EntitySystemsManager.h>
#include <mc/world/actor/Actor.h>
#include <mc/world/level/BlockSource.h>
#include <mc/world/level/chunk/LevelChunk.h>
#include <mc/network/Packet.h>
#include <mc/network/PacketSender.h>
#include <mc/network/LoopbackPacketSender.h>

#include <chrono>
#include <filesystem>
#include <algorithm>

namespace dim_parallel {

static Config                          config;
static std::shared_ptr<ll::io::Logger> log;
static bool                            hookInstalled = false;

static thread_local DimensionWorkerContext* tl_currentContext   = nullptr;
static thread_local bool                   tl_isWorkerThread   = false;
static thread_local int                    tl_currentDimTypeId = -1;
static thread_local const char*            tl_currentPhase     = "idle";

static std::atomic<bool>       g_inParallelPhase{false};
static std::atomic<bool>       g_suppressDimensionTick{false};
static std::vector<Dimension*> g_collectedDimensions;
static std::mutex              g_collectMutex;

// Actor tick 并行控制
static std::atomic<bool> g_suppressActorTick{false};

struct DeferredActorTick {
    Actor*       actor;
    BlockSource* region;
    int          dimId;
};
static std::mutex                       g_actorCollectMutex;
static std::vector<DeferredActorTick>   g_collectedActorTicks;
static std::atomic<bool>                g_inActorParallelPhase{false};

Config& getConfig() { return config; }

ll::io::Logger& logger() {
    if (!log) {
        log = ll::io::LoggerRegistry::getInstance().getOrCreate("DimParallel");
    }
    return *log;
}

bool loadConfig() {
    auto path = PluginImpl::getInstance().getSelf().getConfigDir() / "config.json";
    return ll::config::loadConfig(config, path);
}

bool saveConfig() {
    auto path = PluginImpl::getInstance().getSelf().getConfigDir() / "config.json";
    return ll::config::saveConfig(config, path);
}

void MainThreadTaskQueue::enqueue(std::function<void()> task) {
    std::lock_guard lock(mMutex);
    mTasks.push_back(std::move(task));
}

void MainThreadTaskQueue::processAll() {
    {
        std::lock_guard lock(mMutex);
        mProcessing.swap(mTasks);
    }
    for (auto& task : mProcessing) {
        task();
    }
    mProcessing.clear();
}

size_t MainThreadTaskQueue::size() const {
    std::lock_guard lock(mMutex);
    return mTasks.size();
}

void WorkerPool::start(int numWorkers) {
    std::lock_guard lock(mMutex);
    if (!mWorkers.empty()) return;
    mShutdown   = false;
    mGeneration = 0;
    mBatch.tasks = nullptr;
    mBatch.count = 0;
    mBatch.nextIdx.store(0, std::memory_order_relaxed);
    mBatch.doneCount.store(0, std::memory_order_relaxed);
    for (int i = 0; i < numWorkers; i++) {
        mWorkers.emplace_back([this, i]() { workerLoop(i); });
    }
}

void WorkerPool::stop() {
    {
        std::lock_guard lock(mMutex);
        mShutdown = true;
        mGeneration++;
    }
    mWakeCV.notify_all();
    for (auto& w : mWorkers) {
        if (w.joinable()) w.join();
    }
    mWorkers.clear();
}

void WorkerPool::executeAll(std::vector<std::function<void()>>& tasks) {
    if (tasks.empty()) return;
    int count = static_cast<int>(tasks.size());

    mBatch.tasks = tasks.data();
    mBatch.count = count;
    mBatch.nextIdx.store(0, std::memory_order_relaxed);
    mBatch.doneCount.store(0, std::memory_order_relaxed);

    {
        std::lock_guard lock(mMutex);
        mGeneration++;
    }
    mWakeCV.notify_all();

    while (true) {
        int idx = mBatch.nextIdx.fetch_add(1, std::memory_order_relaxed);
        if (idx >= count) break;
        tasks[idx]();
        mBatch.doneCount.fetch_add(1, std::memory_order_release);
    }

    while (mBatch.doneCount.load(std::memory_order_acquire) < count) {
        std::this_thread::yield();
    }

    mBatch.tasks = nullptr;
    mBatch.count = 0;
}

void WorkerPool::workerLoop(int) {
    uint64_t localGen = 0;
    while (true) {
        {
            std::unique_lock lock(mMutex);
            mWakeCV.wait(lock, [this, &localGen]() {
                return mShutdown || mGeneration > localGen;
            });
            if (mShutdown) return;
            localGen = mGeneration;
        }
        if (mBatch.tasks) {
            int count = mBatch.count;
            while (true) {
                int idx = mBatch.nextIdx.fetch_add(1, std::memory_order_relaxed);
                if (idx >= count) break;
                mBatch.tasks[idx]();
                mBatch.doneCount.fetch_add(1, std::memory_order_release);
            }
        }
    }
}

ParallelDimensionTickManager& ParallelDimensionTickManager::getInstance() {
    static ParallelDimensionTickManager instance;
    return instance;
}

void ParallelDimensionTickManager::initialize() {
    if (mInitialized) return;
    mFallbackToSerial = false;
    int hwThreads  = static_cast<int>(std::thread::hardware_concurrency());
    int numWorkers = std::max(1, hwThreads - 1);
    mPool.start(numWorkers);
    mInitialized = true;
    logger().info("Initialized: {} hw threads, {} workers + main thread", hwThreads, numWorkers);
}

void ParallelDimensionTickManager::shutdown() {
    if (!mInitialized) return;
    mPool.stop();
    mContexts.clear();
    mInitialized = false;
    logger().info("Shutdown");
}

bool ParallelDimensionTickManager::isWorkerThread() { return tl_isWorkerThread; }
DimensionWorkerContext* ParallelDimensionTickManager::getCurrentContext() { return tl_currentContext; }
DimensionType ParallelDimensionTickManager::getCurrentDimensionType() { return DimensionType(tl_currentDimTypeId); }

void ParallelDimensionTickManager::runOnMainThread(std::function<void()> task) {
    if (!tl_isWorkerThread || !tl_currentContext) {
        task();
        return;
    }
    tl_currentContext->mainThreadTasks.enqueue(std::move(task));
}

void ParallelDimensionTickManager::dispatchAndSync(Level* level) {
    if (!level || !mInitialized || mFallbackToSerial) {
        std::vector<Dimension*> dims;
        level->forEachDimension([&](Dimension& dim) -> bool {
            dims.push_back(&dim);
            return true;
        });
        serialFallbackTick(dims);
        return;
    }

    mSnapshot.time      = level->getTime();
    mSnapshot.simPaused = level->getSimPaused();
    if (mSnapshot.simPaused) return;

    auto& dimensions = g_collectedDimensions;
    if (dimensions.empty()) return;

    if (dimensions.size() == 1) {
        dimensions[0]->tick();
        return;
    }

    for (auto* dim : dimensions) {
        int   dimId   = dim->getDimensionId();
        auto& ctx     = mContexts[dimId];
        ctx.dimension = dim;
    }

    std::vector<std::function<void()>> tasks;
    tasks.reserve(dimensions.size());
    for (auto* dim : dimensions) {
        int   dimId = dim->getDimensionId();
        auto& ctx   = mContexts[dimId];
        tasks.emplace_back([this, &ctx]() { tickDimensionOnWorker(ctx); });
    }

    mPool.executeAll(tasks);
    processAllMainThreadTasks();

    mStats.totalParallelTicks++;

    if (config.debug && (mStats.totalParallelTicks % 200 == 0)) {
        logger().info(
            "Parallel tick #{}: dims={}  mainTasks={}  fallbacks={}  workers={}  actorSup={}  actorPar={}",
            mStats.totalParallelTicks.load(),
            dimensions.size(),
            mStats.totalMainThreadTasks.load(),
            mStats.totalFallbackTicks.load(),
            mPool.workerCount(),
            mStats.actorTicksSuppressed.load(),
            mStats.actorTicksParallel.load()
        );
        for (auto& [id, ctx] : mContexts) {
            logger().info("  dim[{}]: {}us", id, ctx.lastTickTimeUs);
        }
    }
}

void ParallelDimensionTickManager::tickDimensionOnWorker(DimensionWorkerContext& ctx) {
    tl_isWorkerThread  = true;
    tl_currentContext   = &ctx;
    tl_currentDimTypeId = ctx.dimension->getDimensionId();
    tl_currentPhase     = "pre-tick";

    auto start = std::chrono::steady_clock::now();

    try {
        tl_currentPhase = "tick";
        ctx.dimension->tick();
        tl_currentPhase = "post-tick";
    } catch (std::exception& e) {
        logger().error("std::exception in dim {} during [{}]: {}", tl_currentDimTypeId, tl_currentPhase, e.what());
        mFallbackToSerial = true;
    } catch (...) {
        logger().error("SEH/unknown exception in dim {} during [{}]", tl_currentDimTypeId, tl_currentPhase);
        mFallbackToSerial = true;
    }

    auto end           = std::chrono::steady_clock::now();
    ctx.lastTickTimeUs = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

    uint64_t expected = mStats.maxDimTickTimeUs.load(std::memory_order_relaxed);
    while (ctx.lastTickTimeUs > expected) {
        if (mStats.maxDimTickTimeUs.compare_exchange_weak(expected, ctx.lastTickTimeUs)) break;
    }

    tl_isWorkerThread  = false;
    tl_currentContext   = nullptr;
    tl_currentDimTypeId = -1;
    tl_currentPhase     = "idle";
}

void ParallelDimensionTickManager::processAllMainThreadTasks() {
    for (auto& [dimId, ctx] : mContexts) {
        size_t count = ctx.mainThreadTasks.size();
        ctx.mainThreadTasks.processAll();
        mStats.totalMainThreadTasks += count;
    }
}

void ParallelDimensionTickManager::serialFallbackTick(std::vector<Dimension*>& dimensions) {
    for (auto* dim : dimensions) {
        dim->tick();
    }
    mStats.totalFallbackTicks++;
}

} // namespace dim_parallel

using namespace dim_parallel;

// ==================== Dimension Hooks ====================

LL_TYPE_INSTANCE_HOOK(
    DimensionTickRedstoneHook,
    ll::memory::HookPriority::Normal,
    Dimension,
    &Dimension::$tickRedstone,
    void
) {
    if (!config.enabled || !ParallelDimensionTickManager::isWorkerThread()) {
        origin();
        return;
    }
    tl_currentPhase = "tickRedstone";
    try { origin(); }
    catch (...) {
        logger().error("Exception in dim {} during tickRedstone", tl_currentDimTypeId);
        throw;
    }
}

LL_TYPE_INSTANCE_HOOK(
    DimensionSendBlocksChangedHook,
    ll::memory::HookPriority::Normal,
    Dimension,
    &Dimension::_sendBlocksChangedPackets,
    void
) {
    if (!config.enabled || !ParallelDimensionTickManager::isWorkerThread()) {
        origin();
        return;
    }
    tl_currentPhase = "_sendBlocksChangedPackets";
    try { origin(); }
    catch (...) {
        logger().error("Exception in dim {} during _sendBlocksChangedPackets", tl_currentDimTypeId);
        throw;
    }
}

LL_TYPE_INSTANCE_HOOK(
    DimensionProcessEntityTransfersHook,
    ll::memory::HookPriority::Normal,
    Dimension,
    &Dimension::_processEntityChunkTransfers,
    void
) {
    if (!config.enabled || !ParallelDimensionTickManager::isWorkerThread()) {
        origin();
        return;
    }
    tl_currentPhase = "_processEntityChunkTransfers";
    try { origin(); }
    catch (...) {
        logger().error("Exception in dim {} during _processEntityChunkTransfers", tl_currentDimTypeId);
        throw;
    }
}

LL_TYPE_INSTANCE_HOOK(
    DimensionTickEntityChunkMovesHook,
    ll::memory::HookPriority::Normal,
    Dimension,
    &Dimension::_tickEntityChunkMoves,
    void
) {
    if (!config.enabled || !ParallelDimensionTickManager::isWorkerThread()) {
        origin();
        return;
    }
    tl_currentPhase = "_tickEntityChunkMoves";
    try { origin(); }
    catch (...) {
        logger().error("Exception in dim {} during _tickEntityChunkMoves", tl_currentDimTypeId);
        throw;
    }
}

LL_TYPE_INSTANCE_HOOK(
    DimensionRunChunkGenWatchdogHook,
    ll::memory::HookPriority::Normal,
    Dimension,
    &Dimension::_runChunkGenerationWatchdog,
    void
) {
    if (!config.enabled || !ParallelDimensionTickManager::isWorkerThread()) {
        origin();
        return;
    }
    Dimension* self = this;
    ParallelDimensionTickManager::runOnMainThread([self]() {
        self->_runChunkGenerationWatchdog();
    });
}

LL_TYPE_INSTANCE_HOOK(
    DimensionTickHook,
    ll::memory::HookPriority::Normal,
    Dimension,
    &Dimension::$tick,
    void
) {
    if (!config.enabled) {
        origin();
        return;
    }
    if (g_suppressDimensionTick.load(std::memory_order_acquire)) {
        std::lock_guard lock(g_collectMutex);
        g_collectedDimensions.push_back(this);
        return;
    }
    if (g_inParallelPhase.load(std::memory_order_acquire)) {
        origin();
        return;
    }
    origin();
}

LL_TYPE_INSTANCE_HOOK(
    DimensionSendBroadcastHook,
    ll::memory::HookPriority::Normal,
    Dimension,
    &Dimension::$sendBroadcast,
    void,
    Packet const& packet,
    Player*       except
) {
    if (!config.enabled || !ParallelDimensionTickManager::isWorkerThread()) {
        origin(packet, except);
        return;
    }
    origin(packet, except);
}

LL_TYPE_INSTANCE_HOOK(
    DimensionSendPacketForPositionHook,
    ll::memory::HookPriority::Normal,
    Dimension,
    &Dimension::$sendPacketForPosition,
    void,
    BlockPos const& position,
    Packet const&   packet,
    Player const*   except
) {
    if (!config.enabled || !ParallelDimensionTickManager::isWorkerThread()) {
        origin(position, packet, except);
        return;
    }
    origin(position, packet, except);
}

LL_TYPE_INSTANCE_HOOK(
    DimensionSendPacketForEntityHook,
    ll::memory::HookPriority::Normal,
    Dimension,
    &Dimension::$sendPacketForEntity,
    void,
    Actor const&  actor,
    Packet const& packet,
    Player const* except
) {
    if (!config.enabled || !ParallelDimensionTickManager::isWorkerThread()) {
        origin(actor, packet, except);
        return;
    }
    origin(actor, packet, except);
}

// ==================== Actor tick 并行化 ====================

// 策略：hook Actor::tick，在 suppress 阶段收集所有 actor tick 调用
// 然后按维度分组，并行执行

LL_TYPE_INSTANCE_HOOK(
    ActorTickHook,
    ll::memory::HookPriority::Normal,
    Actor,
    &Actor::tick,
    bool,
    BlockSource& region
) {
    if (!config.enabled || !config.parallelActorTick) {
        return origin(region);
    }

    if (g_suppressActorTick.load(std::memory_order_acquire)) {
        // 收集阶段：记录 actor 和 region，不执行
        int dimId = static_cast<int>(region.getDimensionId());
        {
            std::lock_guard lock(g_actorCollectMutex);
            g_collectedActorTicks.push_back({this, &region, dimId});
        }
        ParallelDimensionTickManager::getInstance().getStats().actorTicksSuppressed++;
        return false; // 返回 false 表示未处理
    }

    if (g_inActorParallelPhase.load(std::memory_order_acquire)) {
        // 并行执行阶段：正常 tick
        return origin(region);
    }

    return origin(region);
}

// ==================== EntitySystemsManager hook ====================

// Hook tickEntitySystems：收集 actor ticks，按维度并行执行
LL_TYPE_INSTANCE_HOOK(
    EntitySystemsManagerTickHook,
    ll::memory::HookPriority::Normal,
    EntitySystemsManager,
    &EntitySystemsManager::tickEntitySystems,
    void
) {
    if (!config.enabled || !config.parallelActorTick) {
        origin();
        return;
    }

    // 阶段 1：suppress Actor::tick，收集所有 actor tick 调用
    g_collectedActorTicks.clear();
    g_suppressActorTick.store(true, std::memory_order_release);

    // 调用原始 tickEntitySystems
    // 内部的 ECS system 会遍历实体并调用 Actor::tick
    // 我们的 ActorTickHook 会拦截并收集而不执行
    origin();

    g_suppressActorTick.store(false, std::memory_order_release);

    // 阶段 2：按维度分组
    std::unordered_map<int, std::vector<DeferredActorTick>> byDimension;
    for (auto& entry : g_collectedActorTicks) {
        byDimension[entry.dimId].push_back(entry);
    }

    if (byDimension.empty()) return;

    // 如果只有一个维度，串行执行
    if (byDimension.size() == 1) {
        g_inActorParallelPhase.store(true, std::memory_order_release);
        for (auto& entry : g_collectedActorTicks) {
            entry.actor->tick(*entry.region);
        }
        g_inActorParallelPhase.store(false, std::memory_order_release);
        return;
    }

    // 阶段 3：并行执行每个维度的 actor ticks
    std::vector<std::function<void()>> tasks;
    tasks.reserve(byDimension.size());

    auto& stats = ParallelDimensionTickManager::getInstance().getStats();

    for (auto& [dimId, actors] : byDimension) {
        tasks.emplace_back([&actors, &stats]() {
            for (auto& entry : actors) {
                try {
                    entry.actor->tick(*entry.region);
                    stats.actorTicksParallel++;
                } catch (...) {
                    // 单个 actor 崩溃不应该影响其他
                }
            }
        });
    }

    g_inActorParallelPhase.store(true, std::memory_order_release);
    ParallelDimensionTickManager::getInstance().getWorkerPool().executeAll(tasks);
    g_inActorParallelPhase.store(false, std::memory_order_release);
}

// ==================== Level tick hook ====================

LL_TYPE_INSTANCE_HOOK(
    LevelTickHook,
    ll::memory::HookPriority::Normal,
    Level,
    &Level::$tick,
    void
) {
    if (!config.enabled) {
        origin();
        return;
    }

    g_collectedDimensions.clear();
    g_suppressDimensionTick.store(true, std::memory_order_release);

    origin();

    g_suppressDimensionTick.store(false, std::memory_order_release);

    if (!g_collectedDimensions.empty()) {
        g_inParallelPhase.store(true, std::memory_order_release);
        ParallelDimensionTickManager::getInstance().dispatchAndSync(this);
        g_inParallelPhase.store(false, std::memory_order_release);
    }
}

// ==================== 插件生命周期 ====================

namespace dim_parallel {

PluginImpl& PluginImpl::getInstance() {
    static PluginImpl instance;
    return instance;
}

bool PluginImpl::load() {
    std::filesystem::create_directories(getSelf().getConfigDir());
    if (!loadConfig()) {
        logger().warn("Failed to load config, using defaults");
        saveConfig();
    }
    logger().info("DimParallel loaded. enabled={} debug={} actorTick={} chunkTick={}",
        config.enabled, config.debug, config.parallelActorTick, config.parallelChunkTick);
    return true;
}

bool PluginImpl::enable() {
    if (!hookInstalled) {
        LevelTickHook::hook();
        DimensionTickHook::hook();
        DimensionTickRedstoneHook::hook();
        DimensionSendBlocksChangedHook::hook();
        DimensionProcessEntityTransfersHook::hook();
        DimensionTickEntityChunkMovesHook::hook();
        DimensionRunChunkGenWatchdogHook::hook();
        DimensionSendBroadcastHook::hook();
        DimensionSendPacketForPositionHook::hook();
        DimensionSendPacketForEntityHook::hook();

        if (config.parallelActorTick) {
            ActorTickHook::hook();
            EntitySystemsManagerTickHook::hook();
        }

        hookInstalled = true;
    }
    ParallelDimensionTickManager::getInstance().initialize();
    logger().info("DimParallel enabled");
    return true;
}

bool PluginImpl::disable() {
    ParallelDimensionTickManager::getInstance().shutdown();
    if (hookInstalled) {
        LevelTickHook::unhook();
        DimensionTickHook::unhook();
        DimensionTickRedstoneHook::unhook();
        DimensionSendBlocksChangedHook::unhook();
        DimensionProcessEntityTransfersHook::unhook();
        DimensionTickEntityChunkMovesHook::unhook();
        DimensionRunChunkGenWatchdogHook::unhook();
        DimensionSendBroadcastHook::unhook();
        DimensionSendPacketForPositionHook::unhook();
        DimensionSendPacketForEntityHook::unhook();

        if (config.parallelActorTick) {
            ActorTickHook::unhook();
            EntitySystemsManagerTickHook::unhook();
        }

        hookInstalled = false;
    }
    logger().info("DimParallel disabled");
    return true;
}

} // namespace dim_parallel

LL_REGISTER_MOD(dim_parallel::PluginImpl, dim_parallel::PluginImpl::getInstance());

