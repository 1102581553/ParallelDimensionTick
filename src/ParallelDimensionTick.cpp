#include "ParallelDimensionTick.h"

#include <ll/api/memory/Hook.h>
#include <ll/api/mod/RegisterHelper.h>
#include <ll/api/io/LoggerRegistry.h>

#include <mc/world/level/Level.h>
#include <mc/server/ServerLevel.h>
#include <mc/world/level/dimension/Dimension.h>
#include <mc/world/level/Tick.h>
#include <mc/world/level/BlockSource.h>
#include <mc/world/actor/Actor.h>
#include <mc/network/Packet.h>
#include <mc/network/PacketSender.h>
#include <mc/network/LoopbackPacketSender.h>

#include <Windows.h>
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

unsigned long __stdcall WorkerPool::threadEntry(void* param) {
    auto* pool = static_cast<WorkerPool*>(param);
    pool->workerLoop();
    return 0;
}

void WorkerPool::start(int numWorkers) {
    std::lock_guard lock(mMutex);
    if (!mHandles.empty()) return;
    mShutdown   = false;
    mGeneration = 0;
    mBatch.tasks = nullptr;
    mBatch.count = 0;
    mBatch.nextIdx.store(0, std::memory_order_relaxed);
    mBatch.doneCount.store(0, std::memory_order_relaxed);
    for (int i = 0; i < numWorkers; i++) {
        void* h = CreateThread(
            nullptr,
            8 * 1024 * 1024,
            threadEntry,
            this,
            0,
            nullptr
        );
        if (h) mHandles.push_back(h);
    }
}

void WorkerPool::stop() {
    {
        std::lock_guard lock(mMutex);
        mShutdown = true;
        mGeneration++;
    }
    mWakeCV.notify_all();
    for (auto h : mHandles) {
        WaitForSingleObject(h, INFINITE);
        CloseHandle(h);
    }
    mHandles.clear();
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

void WorkerPool::workerLoop() {
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

    std::vector<std::function<void()>> dimTasks;
    dimTasks.reserve(dimensions.size());
    for (auto* dim : dimensions) {
        int   dimId = dim->getDimensionId();
        auto& ctx   = mContexts[dimId];
        dimTasks.emplace_back([this, &ctx]() { tickDimensionOnWorker(ctx); });
    }

    mPool.executeAll(dimTasks);
    processAllMainThreadTasks();

    mStats.totalParallelTicks++;

    if (config.debug && (mStats.totalParallelTicks % 200 == 0)) {
        logger().info(
            "Parallel tick #{}: dims={}  mainTasks={}  fallbacks={}  workers={}",
            mStats.totalParallelTicks.load(),
            dimensions.size(),
            mStats.totalMainThreadTasks.load(),
            mStats.totalFallbackTicks.load(),
            mPool.workerCount()
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
    // 提交到主线程执行
    ParallelDimensionTickManager::runOnMainThread([this]() {
        this->origin();
    });
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
    ParallelDimensionTickManager::runOnMainThread([this]() {
        this->origin();
    });
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
    ParallelDimensionTickManager::runOnMainThread([this]() {
        this->origin();
    });
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
    ParallelDimensionTickManager::runOnMainThread([this]() {
        this->origin();
    });
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
    // 已经走主线程
    ParallelDimensionTickManager::runOnMainThread([this]() {
        this->origin();
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
    // 提交到主线程，注意捕获参数（引用捕获，需确保生命周期）
    ParallelDimensionTickManager::runOnMainThread([this, &packet, except]() {
        this->origin(packet, except);
    });
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
    // 按值捕获 position（可复制），packet 仍为引用
    ParallelDimensionTickManager::runOnMainThread([this, position, &packet, except]() {
        this->origin(position, packet, except);
    });
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
    // 捕获引用，需保证 actor 和 packet 在任务执行时有效
    ParallelDimensionTickManager::runOnMainThread([this, &actor, &packet, except]() {
        this->origin(actor, packet, except);
    });
}

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
    logger().info("DimParallel loaded. enabled={} debug={}", config.enabled, config.debug);
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
        hookInstalled = false;
    }
    logger().info("DimParallel disabled");
    return true;
}

} // namespace dim_parallel

LL_REGISTER_MOD(dim_parallel::PluginImpl, dim_parallel::PluginImpl::getInstance());
