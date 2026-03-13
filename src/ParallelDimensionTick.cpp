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

static Config config;
static std::shared_ptr<ll::io::Logger> log;
static bool hookInstalled = false;

static thread_local DimensionWorkerContext* tl_currentContext = nullptr;
static thread_local bool tl_isWorkerThread = false;
static thread_local int tl_currentDimTypeId = -1;
static thread_local const char* tl_currentPhase = "idle";

static std::atomic<bool> g_inParallelPhase{false};
static std::atomic<bool> g_suppressDimensionTick{false};

static std::vector<Dimension*> g_collectedDimensions;

std::unordered_set<std::string> ParallelDimensionTickManager::m_dangerousFunctions;
std::mutex ParallelDimensionTickManager::m_dangerousMutex;

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
    logger().info("Initialized independent per-dimension thread model");
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
    tl_currentContext->mainThreadTasks.enqueue(std::move(task));
}

void ParallelDimensionTickManager::markFunctionDangerous(const std::string& funcName) {
    std::lock_guard lock(m_dangerousMutex);
    if (m_dangerousFunctions.insert(funcName).second) {
        logger().warn("Function '{}' marked as dangerous (will run on main thread)", funcName);
        getInstance().mStats.totalDangerousFunctions++;
    }
}

bool ParallelDimensionTickManager::isFunctionDangerous(const std::string& funcName) {
    std::lock_guard lock(m_dangerousMutex);
    return m_dangerousFunctions.find(funcName) != m_dangerousFunctions.end();
}

void ParallelDimensionTickManager::workerLoop(DimensionWorkerContext* ctx) {
    tl_isWorkerThread = true;
    tl_currentContext = ctx;
    if (ctx->dimensionPtr) {
        tl_currentDimTypeId = ctx->dimensionPtr->getDimensionId();
    } else {
        tl_currentDimTypeId = -1;
    }

    while (true) {
        std::unique_lock lock(ctx->wakeMutex);
        ctx->wakeCV.wait(lock, [ctx] { return ctx->shouldWork || ctx->shutdown; });
        if (ctx->shutdown) break;
        ctx->shouldWork = false;
        lock.unlock();

        if (ctx->dimensionPtr) {
            tickDimensionOnWorker(*ctx);
        } else {
            logger().error("Dimension worker: dimension pointer is null, skipping tick");
        }
        
        ctx->isProcessing.store(false, std::memory_order_release);
        ctx->tickCompleted.store(true, std::memory_order_release);
    }

    tl_isWorkerThread = false;
    tl_currentContext = nullptr;
    tl_currentDimTypeId = -1;
    tl_currentPhase = "idle";
}

void ParallelDimensionTickManager::dispatchAndSync(Level* level) {
    if (!level || !mInitialized) {
        std::vector<Dimension*> dimRefs;
        level->forEachDimension([&](Dimension& dim) -> bool {
            dimRefs.emplace_back(&dim);
            return true;
        });
        serialFallbackTick(dimRefs);
        return;
    }

    mSnapshot.time = level->getTime();
    mSnapshot.simPaused = level->getSimPaused();
    if (mSnapshot.simPaused) return;

    auto& dimRefs = g_collectedDimensions;
    if (dimRefs.empty()) return;

    std::vector<Dimension*> validDims;
    for (auto* dim : dimRefs) {
        if (dim) {
            validDims.push_back(dim);
        } else {
            if (config.debug) {
                logger().info("Skipping null dimension pointer");
            }
            mStats.totalSkippedDimensions++;
        }
    }

    if (validDims.empty()) return;

    if (validDims.size() == 1) {
        validDims[0]->tick();
        return;
    }

    if (mFallbackToSerial.load(std::memory_order_relaxed)) {
        uint64_t currentTick = level->getTime();
        if (currentTick - mFallbackStartTick >= RECOVERY_INTERVAL_TICKS) {
            mStats.totalRecoveryAttempts++;
            if (config.debug) {
                logger().info("Attempting recovery from fallback mode at tick {}", currentTick);
            }
            mFallbackToSerial.store(false, std::memory_order_relaxed);
        } else {
            serialFallbackTick(dimRefs);
            return;
        }
    }

    // 初始化或更新 context
    for (auto* dim : validDims) {
        int dimId = dim->getDimensionId();
        auto it = mContexts.find(dimId);
        if (it == mContexts.end()) {
            auto ctx = std::make_unique<DimensionWorkerContext>();
            ctx->dimensionPtr = dim;
            ctx->tickCompleted = false;
            ctx->isProcessing = false;
            ctx->shutdown = false;
            ctx->shouldWork = false;
            ctx->tickNumber = 0;
            ctx->skippedTicks = 0;
            ctx->totalSkippedTicks = 0;
            ctx->workerThread = std::thread(&ParallelDimensionTickManager::workerLoop, this, ctx.get());
            mContexts[dimId] = std::move(ctx);
        } else {
            it->second->dimensionPtr = dim;
        }
    }

    // 异步启动所有维度 tick（不等待完成）
    for (auto* dim : validDims) {
        int dimId = dim->getDimensionId();
        auto& ctx = *mContexts[dimId];
        
        // 如果上一个 tick 还没完成，跳过本次（维度独立运行）
        if (ctx.isProcessing.load(std::memory_order_acquire)) {
            ctx.skippedTicks++;
            ctx.totalSkippedTicks++;
            mStats.totalTicksSkippedDueToBacklog++;
            
            if (config.debug && ctx.skippedTicks % 20 == 0) {
                logger().warn("Dimension {} has skipped {} ticks (total: {})", 
                    dimId, ctx.skippedTicks, ctx.totalSkippedTicks);
            }
            continue;
        }

        // 重置连续跳过计数
        if (ctx.skippedTicks > 0) {
            if (config.debug) {
                logger().info("Dimension {} recovered after {} skipped ticks", dimId, ctx.skippedTicks);
            }
            ctx.skippedTicks = 0;
        }

        ctx.tickCompleted.store(false, std::memory_order_release);
        ctx.isProcessing.store(true, std::memory_order_release);
        ctx.tickNumber++;
        
        {
            std::lock_guard lock(ctx.wakeMutex);
            ctx.shouldWork = true;
        }
        ctx.wakeCV.notify_one();
    }

    // 非阻塞式处理已完成的维度任务
    uint64_t totalTasksThisCycle = 0;
    for (auto* dim : validDims) {
        int dimId = dim->getDimensionId();
        auto& ctx = *mContexts[dimId];
        
        if (ctx.tickCompleted.load(std::memory_order_acquire)) {
            size_t taskCount = ctx.mainThreadTasks.size();
            if (taskCount > 0) {
                ctx.mainThreadTasks.processAll();
                totalTasksThisCycle += taskCount;
            }
            ctx.tickCompleted.store(false, std::memory_order_release);
        }
    }

    if (totalTasksThisCycle > 0) {
        mStats.totalMainThreadTasks += totalTasksThisCycle;
        mStats.cycleMainThreadTasks += totalTasksThisCycle;
    }

    if (mFallbackToSerial.load(std::memory_order_relaxed)) {
        mFallbackStartTick = level->getTime();
    }

    mStats.totalParallelTicks++;

    if (config.debug && (mStats.totalParallelTicks % 200 == 0)) {
        uint64_t cycleTasks = mStats.cycleMainThreadTasks.exchange(0);
        logger().info(
            "Parallel tick #{}: dims={} mainTasks={} skippedTicksTotal={} fallbacks={} dangerous={} recoveryAttempts={} skipped={}",
            mStats.totalParallelTicks.load(),
            validDims.size(),
            cycleTasks,
            mStats.totalTicksSkippedDueToBacklog.load(),
            mStats.totalFallbackTicks.load(),
            mStats.totalDangerousFunctions.load(),
            mStats.totalRecoveryAttempts.load(),
            mStats.totalSkippedDimensions.load()
        );
        for (auto* dim : validDims) {
            int dimId = dim->getDimensionId();
            auto& ctx = *mContexts[dimId];
            logger().info(" dim[{}]: {}us (tick #{}, skipped: {})", 
                dimId, ctx.lastTickTimeUs, ctx.tickNumber, ctx.totalSkippedTicks);
        }
    }
}

void ParallelDimensionTickManager::tickDimensionOnWorker(DimensionWorkerContext& ctx) {
    tl_isWorkerThread = true;
    tl_currentContext = &ctx;
    if (ctx.dimensionPtr) {
        tl_currentDimTypeId = ctx.dimensionPtr->getDimensionId();
    } else {
        tl_currentDimTypeId = -1;
    }

    tl_currentPhase = "pre-tick";
    auto start = std::chrono::steady_clock::now();

    try {
        tl_currentPhase = "tick";
        if (ctx.dimensionPtr) {
            ctx.dimensionPtr->tick();
        }
        tl_currentPhase = "post-tick";
    } catch (std::exception& e) {
        logger().error("std::exception in dim {} during [{}]: {}", tl_currentDimTypeId, tl_currentPhase, e.what());
        if (tl_currentPhase && tl_currentPhase[0] != '\0' &&
            strcmp(tl_currentPhase, "pre-tick") != 0 && strcmp(tl_currentPhase, "post-tick") != 0) {
            markFunctionDangerous(tl_currentPhase);
        }
        mFallbackToSerial.store(true, std::memory_order_relaxed);
    } catch (...) {
        logger().error("SEH/unknown exception in dim {} during [{}]", tl_currentDimTypeId, tl_currentPhase);
        if (tl_currentPhase && tl_currentPhase[0] != '\0' &&
            strcmp(tl_currentPhase, "pre-tick") != 0 && strcmp(tl_currentPhase, "post-tick") != 0) {
            markFunctionDangerous(tl_currentPhase);
        }
        mFallbackToSerial.store(true, std::memory_order_relaxed);
    }

    auto end = std::chrono::steady_clock::now();
    ctx.lastTickTimeUs = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

    uint64_t expected = mStats.maxDimTickTimeUs.load(std::memory_order_relaxed);
    while (ctx.lastTickTimeUs > expected) {
        if (mStats.maxDimTickTimeUs.compare_exchange_weak(expected, ctx.lastTickTimeUs)) break;
    }

    tl_currentPhase = "idle";
}

void ParallelDimensionTickManager::serialFallbackTick(const std::vector<Dimension*>& dimRefs) {
    for (auto* dim : dimRefs) {
        if (dim) dim->tick();
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
            auto bound = std::bind(std::forward<Func>(func), std::forward<Args>(args)...);
            ParallelDimensionTickManager::runOnMainThread([bound = std::move(bound)]() mutable {
                bound();
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
    LevelTickHook,
    ll::memory::HookPriority::Normal,
    Level,
    &Level::$tick,
    void
) {
    static thread_local bool inHook = false;
    if (!config.enabled) {
        origin();
        return;
    }
    if (inHook) {
        origin();
        return;
    }
    inHook = true;

    g_collectedDimensions.clear();
    g_suppressDimensionTick.store(true, std::memory_order_release);

    this->forEachDimension([&](Dimension& dim) -> bool {
        g_collectedDimensions.emplace_back(&dim);
        return true;
    });

    origin();

    g_suppressDimensionTick.store(false, std::memory_order_release);

    if (!g_collectedDimensions.empty()) {
        g_inParallelPhase.store(true, std::memory_order_release);
        ParallelDimensionTickManager::getInstance().dispatchAndSync(this);
        g_inParallelPhase.store(false, std::memory_order_release);
    }

    inHook = false;
}

LL_TYPE_INSTANCE_HOOK(
    DimensionTickHook,
    ll::memory::HookPriority::Normal,
    Dimension,
    &Dimension::$tick,
    void
) {
    static thread_local bool inHook = false;
    if (!config.enabled) {
        origin();
        return;
    }
    if (g_suppressDimensionTick.load(std::memory_order_acquire)) {
        return;
    }
    if (g_inParallelPhase.load(std::memory_order_acquire)) {
        origin();
        return;
    }
    if (inHook) {
        origin();
        return;
    }
    inHook = true;
    origin();
    inHook = false;
}

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
    Player* except
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
    Packet const& packet,
    Player const* except
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
    Actor const& actor,
    Packet const& packet,
    Player const* except
) {
    const char* funcName = "sendPacketForEntity";
    tl_currentPhase = funcName;
    handleDangerousFunction(funcName, [this, &actor, &packet, except]() { origin(actor, packet, except); });
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
