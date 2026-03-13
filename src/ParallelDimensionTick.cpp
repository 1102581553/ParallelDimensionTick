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

//=============================================================================
// ParallelDimensionTickManager implementation with per-dimension threads
//=============================================================================

ParallelDimensionTickManager& ParallelDimensionTickManager::getInstance() {
    static ParallelDimensionTickManager instance;
    return instance;
}

void ParallelDimensionTickManager::initialize() {
    if (mInitialized) return;
    mFallbackToSerial = false;
    mFallbackStartTick = 0;
    mInitialized = true;
    logger().info("Initialized per-dimension thread model, recovery interval = {} ticks", RECOVERY_INTERVAL_TICKS);
}

void ParallelDimensionTickManager::shutdown() {
    if (!mInitialized) return;

    // Signal all worker threads to shut down and wait for them
    for (auto& [id, ctx] : mContexts) {
        if (ctx->workerThread.joinable()) {
            {
                std::lock_guard lock(ctx->wakeMutex);
                ctx->shutdown = true;
                ctx->shouldWork = false;
            }
            ctx->wakeCV.notify_one();
            ctx->workerThread.join();
        }
    }
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
    // Push to global main thread task queue
    mMainThreadTasks.enqueue(std::move(task));
}

void ParallelDimensionTickManager::workerLoop(DimensionWorkerContext* ctx) {
    tl_isWorkerThread  = true;
    tl_currentContext  = ctx;
    tl_currentDimTypeId = ctx->dimension->getDimensionId();

    while (true) {
        std::unique_lock lock(ctx->wakeMutex);
        ctx->wakeCV.wait(lock, [ctx] { return ctx->shouldWork || ctx->shutdown; });
        if (ctx->shutdown) break;
        ctx->shouldWork = false;
        lock.unlock();

        // Perform the dimension tick
        tickDimensionOnWorker(*ctx);

        // Mark completion
        ctx->tickCompleted = true;
    }

    tl_isWorkerThread  = false;
    tl_currentContext  = nullptr;
    tl_currentDimTypeId = -1;
}

void ParallelDimensionTickManager::dispatchAndSync(Level* level) {
    if (!level || !mInitialized) {
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

    // If only one dimension, just tick it directly (no parallelism overhead)
    if (dimensions.size() == 1) {
        dimensions[0]->tick();
        return;
    }

    // Check if we are in fallback mode and attempt recovery if interval elapsed
    if (mFallbackToSerial.load(std::memory_order_relaxed)) {
        uint64_t currentTick = level->getTime();
        if (currentTick - mFallbackStartTick >= RECOVERY_INTERVAL_TICKS) {
            // Attempt recovery
            mStats.totalRecoveryAttempts++;
            logger().debug("Attempting recovery from fallback mode at tick {}", currentTick);
            mFallbackToSerial.store(false, std::memory_order_relaxed);
            // Proceed to parallel execution; if it fails, fallback will be triggered again
        } else {
            serialFallbackTick(dimensions);
            return;
        }
    }

    // Ensure each dimension has a worker thread
    for (auto* dim : dimensions) {
        int dimId = dim->getDimensionId();
        auto it = mContexts.find(dimId);
        if (it == mContexts.end()) {
            auto ctx = std::make_unique<DimensionWorkerContext>();
            ctx->dimension = dim;
            ctx->tickCompleted = false;
            ctx->shutdown = false;
            ctx->shouldWork = false;
            // Start the worker thread
            ctx->workerThread = std::thread(&ParallelDimensionTickManager::workerLoop, this, ctx.get());
            mContexts[dimId] = std::move(ctx);
            it = mContexts.find(dimId);
        } else {
            // Update dimension pointer (in case dimension object changed? unlikely)
            it->second->dimension = dim;
        }
    }

    // Reset completion flags and set work flags
    std::atomic<int> pendingCount(dimensions.size());
    for (auto* dim : dimensions) {
        int dimId = dim->getDimensionId();
        auto& ctx = mContexts[dimId];
        ctx->tickCompleted = false;
        {
            std::lock_guard lock(ctx->wakeMutex);
            ctx->shouldWork = true;
        }
        ctx->wakeCV.notify_one();
    }

    // Wait for all dimensions to complete their tick
    // Simple busy-wait with yield (could be optimized with condition variable, but this is fine)
    while (pendingCount.load(std::memory_order_relaxed) > 0) {
        for (auto* dim : dimensions) {
            int dimId = dim->getDimensionId();
            auto& ctx = mContexts[dimId];
            if (ctx->tickCompleted.load(std::memory_order_acquire)) {
                bool expected = true;
                if (ctx->tickCompleted.compare_exchange_strong(expected, false)) {
                    pendingCount.fetch_sub(1, std::memory_order_relaxed);
                }
            }
        }
        if (pendingCount.load() > 0) {
            std::this_thread::yield();
        }
    }

    // Process all main thread tasks that were queued during parallel tick
    processAllMainThreadTasks();

    // If any dimension tick triggered fallback flag, we switch to serial mode and record start tick
    if (mFallbackToSerial.load(std::memory_order_relaxed)) {
        mFallbackStartTick = level->getTime();
    }

    mStats.totalParallelTicks++;

    if (config.debug && (mStats.totalParallelTicks % 200 == 0)) {
        logger().info(
            "Parallel tick #{}: dims={}  mainTasks={}  fallbacks={}  recoveryAttempts={}",
            mStats.totalParallelTicks.load(),
            dimensions.size(),
            mStats.totalMainThreadTasks.load(),
            mStats.totalFallbackTicks.load(),
            mStats.totalRecoveryAttempts.load()
        );
        for (auto& [id, ctx] : mContexts) {
            logger().info("  dim[{}]: {}us", id, ctx->lastTickTimeUs);
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
        mFallbackToSerial.store(true, std::memory_order_relaxed);
    } catch (...) {
        logger().error("SEH/unknown exception in dim {} during [{}]", tl_currentDimTypeId, tl_currentPhase);
        mFallbackToSerial.store(true, std::memory_order_relaxed);
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
    size_t count = mMainThreadTasks.size();
    mMainThreadTasks.processAll();
    mStats.totalMainThreadTasks += count;
}

void ParallelDimensionTickManager::serialFallbackTick(std::vector<Dimension*>& dimensions) {
    for (auto* dim : dimensions) {
        dim->tick();
    }
    mStats.totalFallbackTicks++;
}

//=============================================================================
// Hooks
//=============================================================================

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

// Modified: Forward _processEntityChunkTransfers to main thread when in worker
LL_TYPE_INSTANCE_HOOK(
    DimensionProcessEntityTransfersHook,
    ll::memory::HookPriority::Normal,
    Dimension,
    &Dimension::_processEntityChunkTransfers,
    void
) {
    if (!config.enabled) {
        origin();
        return;
    }
    if (ParallelDimensionTickManager::isWorkerThread()) {
        // Forward to main thread
        Dimension* self = this;
        ParallelDimensionTickManager::runOnMainThread([self]() {
            self->_processEntityChunkTransfers();
        });
        return;
    }
    origin();
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

//=============================================================================
// Plugin Implementation
//=============================================================================

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
