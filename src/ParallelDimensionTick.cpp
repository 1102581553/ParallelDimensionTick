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
#include <unordered_set>

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

static std::unordered_set<std::string> g_dangerousFunctions;
static std::mutex                       g_dangerousMutex;

MainThreadTaskQueue ParallelDimensionTickManager::mMainThreadTasks;

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
// ParallelDimensionTickManager implementation
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
    mMainThreadTasks.enqueue(std::move(task));
}

void ParallelDimensionTickManager::markFunctionDangerous(const std::string& funcName) {
    std::lock_guard lock(g_dangerousMutex);
    if (g_dangerousFunctions.insert(funcName).second) {
        logger().warn("Function '{}' marked as dangerous (will run on main thread from now on)", funcName);
        getInstance().mStats.totalDangerousFunctions++;
    }
}

bool ParallelDimensionTickManager::isFunctionDangerous(const std::string& funcName) {
    std::lock_guard lock(g_dangerousMutex);
    return g_dangerousFunctions.find(funcName) != g_dangerousFunctions.end();
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

        tickDimensionOnWorker(*ctx);

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

    if (dimensions.size() == 1) {
        dimensions[0]->tick();
        return;
    }

    if (mFallbackToSerial.load(std::memory_order_acquire)) {
        uint64_t currentTick = level->getTime();
        if (currentTick - mFallbackStartTick >= RECOVERY_INTERVAL_TICKS) {
            mStats.totalRecoveryAttempts++;
            logger().debug("Attempting recovery from fallback mode at tick {}", currentTick);
            mFallbackToSerial.store(false, std::memory_order_release);
        } else {
            serialFallbackTick(dimensions);
            return;
        }
    }

    for (auto* dim : dimensions) {
        int dimId = dim->getDimensionId();
        auto it = mContexts.find(dimId);
        if (it == mContexts.end()) {
            auto ctx = std::make_unique<DimensionWorkerContext>();
            ctx->dimension = dim;
            ctx->tickCompleted = false;
            ctx->shutdown = false;
            ctx->shouldWork = false;
            ctx->workerThread = std::thread(&ParallelDimensionTickManager::workerLoop, this, ctx.get());
            mContexts[dimId] = std::move(ctx);
            it = mContexts.find(dimId);
        } else {
            it->second->dimension = dim;
        }
    }

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

    processAllMainThreadTasks();

    if (mFallbackToSerial.load(std::memory_order_acquire)) {
        mFallbackStartTick = level->getTime();
    }

    mStats.totalParallelTicks++;

    if (config.debug && (mStats.totalParallelTicks % 200 == 0)) {
        logger().info(
            "Parallel tick #{}: dims={}  mainTasks={}  fallbacks={}  dangerous={}  recoveryAttempts={}",
            mStats.totalParallelTicks.load(),
            dimensions.size(),
            mStats.totalMainThreadTasks.load(),
            mStats.totalFallbackTicks.load(),
            mStats.totalDangerousFunctions.load(),
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
    bool exceptionOccurred = false;

    try {
        tl_currentPhase = "tick";
        ctx.dimension->tick();
        tl_currentPhase = "post-tick";
    } catch (std::exception& e) {
        exceptionOccurred = true;
        logger().error("std::exception in dim {} during [{}]: {}", tl_currentDimTypeId, tl_currentPhase, e.what());
        if (tl_currentPhase != nullptr && tl_currentPhase[0] != '\0' && 
            strcmp(tl_currentPhase, "pre-tick") != 0 && strcmp(tl_currentPhase, "post-tick") != 0) {
            markFunctionDangerous(tl_currentPhase);
        }
    } catch (...) {
        exceptionOccurred = true;
        logger().error("SEH/unknown exception in dim {} during [{}]", tl_currentDimTypeId, tl_currentPhase);
        if (tl_currentPhase != nullptr && tl_currentPhase[0] != '\0' && 
            strcmp(tl_currentPhase, "pre-tick") != 0 && strcmp(tl_currentPhase, "post-tick") != 0) {
            markFunctionDangerous(tl_currentPhase);
        }
    }

    if (exceptionOccurred) {
        mFallbackToSerial.store(true, std::memory_order_release);
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
    mStats.totalMainThreadTasks += static_cast<uint64_t>(count);
}

void ParallelDimensionTickManager::serialFallbackTick(std::vector<Dimension*>& dimensions) {
    for (auto* dim : dimensions) {
        dim->tick();
    }
    mStats.totalFallbackTicks++;
}

//=============================================================================
// Helper template for dangerous function forwarding
//=============================================================================

template<typename Func, typename... Args>
inline void handleDangerousFunction(const char* funcName, Func&& func, Args&&... args) {
    if (!config.enabled) {
        std::forward<Func>(func)(std::forward<Args>(args)...);
        return;
    }
    if (ParallelDimensionTickManager::isWorkerThread()) {
        if (ParallelDimensionTickManager::isFunctionDangerous(funcName)) {
            ParallelDimensionTickManager::runOnMainThread([funcName, func = std::forward<Func>(func), 
                                                           ...capturedArgs = std::forward<Args>(args)]() mutable {
                func(std::forward<decltype(capturedArgs)>(capturedArgs)...);
            });
            return;
        }
    }
    std::forward<Func>(func)(std::forward<Args>(args)...);
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
    const char* funcName = "tickRedstone";
    tl_currentPhase = funcName;
    handleDangerousFunction(funcName, [this]() { origin(); });
}

LL_TYPE_INSTANCE_HOOK(
    DimensionSendBlocksChangedHook,
    ll::memory::HookPriority::Normal,
    Dimension,
    &Dimension::_sendBlocksChangedPackets,
    void
) {
    const char* funcName = "_sendBlocksChangedPackets";
    tl_currentPhase = funcName;
    handleDangerousFunction(funcName, [this]() { origin(); });
}

LL_TYPE_INSTANCE_HOOK(
    DimensionProcessEntityTransfersHook,
    ll::memory::HookPriority::Normal,
    Dimension,
    &Dimension::_processEntityChunkTransfers,
    void
) {
    const char* funcName = "_processEntityChunkTransfers";
    tl_currentPhase = funcName;
    handleDangerousFunction(funcName, [this]() { origin(); });
}

LL_TYPE_INSTANCE_HOOK(
    DimensionTickEntityChunkMovesHook,
    ll::memory::HookPriority::Normal,
    Dimension,
    &Dimension::_tickEntityChunkMoves,
    void
) {
    const char* funcName = "_tickEntityChunkMoves";
    tl_currentPhase = funcName;
    handleDangerousFunction(funcName, [this]() { origin(); });
}

LL_TYPE_INSTANCE_HOOK(
    DimensionRunChunkGenWatchdogHook,
    ll::memory::HookPriority::Normal,
    Dimension,
    &Dimension::_runChunkGenerationWatchdog,
    void
) {
    const char* funcName = "_runChunkGenerationWatchdog";
    tl_currentPhase = funcName;
    handleDangerousFunction(funcName, [this]() { origin(); });
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
    const char* funcName = "sendBroadcast";
    tl_currentPhase = funcName;
    handleDangerousFunction(funcName, [this, &packet, except]() { origin(packet, except); });
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
    const char* funcName = "sendPacketForPosition";
    tl_currentPhase = funcName;
    handleDangerousFunction(funcName, [this, &position, &packet, except]() { origin(position, packet, except); });
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
    const char* funcName = "sendPacketForEntity";
    tl_currentPhase = funcName;
    handleDangerousFunction(funcName, [this, &actor, &packet, except]() { origin(actor, packet, except); });
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

    {
        std::lock_guard lock(g_collectMutex);
        g_collectedDimensions.clear();
    }
    g_suppressDimensionTick.store(true, std::memory_order_release);

    origin();

    g_suppressDimensionTick.store(false, std::memory_order_release);

    std::vector<Dimension*> dims;
    {
        std::lock_guard lock(g_collectMutex);
        dims = std::move(g_collectedDimensions);
    }

    if (!dims.empty()) {
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
