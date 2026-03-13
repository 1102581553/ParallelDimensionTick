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
static thread_local std::string tl_currentPhase = "idle";

static std::atomic<bool> g_inParallelPhase{false};
static std::atomic<bool> g_suppressDimensionTick{false};

static std::vector<Dimension*> g_collectedDimensions;
static std::mutex g_dimensionCollectionMutex;

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

//=============================================================================
// ParallelDimensionTickManager
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
    logger().info("已初始化异步独立维度模型，支持 SEH 自适应");
}

void ParallelDimensionTickManager::shutdown() {
    if (!mInitialized) return;
    for (auto& [id, ctx] : mContexts) {
        if (ctx->workerThread.joinable()) {
            ctx->shutdown.store(true, std::memory_order_release);
            ctx->wakeCV.notify_one();
            ctx->workerThread.join();
        }
    }
    mContexts.clear();
    mInitialized = false;
    logger().info("已关闭");
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
        logger().warn("自动适应：'{}' 已标记为危险（将转发到主线程）", funcName);
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
    tl_currentDimTypeId = -1;

    while (true) {
        std::unique_lock lock(ctx->wakeMutex);
        ctx->wakeCV.wait(lock, [ctx] {
            return ctx->shouldWork.load(std::memory_order_acquire) ||
                   ctx->shutdown.load(std::memory_order_acquire);
        });

        if (ctx->shutdown.load(std::memory_order_acquire)) break;

        ctx->shouldWork.store(false, std::memory_order_release);
        lock.unlock();

        if (ctx->dimensionPtr) {
            tl_currentDimTypeId = ctx->dimensionPtr->getDimensionId();
        }
        tl_currentContext = ctx;

        if (ctx->dimensionPtr) {
            tickDimensionOnWorker(*ctx);
        } else {
            logger().error("维度工作线程：空维度指针");
        }

        ctx->isProcessing.store(false, std::memory_order_release);
        ctx->tickCompleted.store(true, std::memory_order_release);
    }

    tl_isWorkerThread = false;
    tl_currentContext = nullptr;
    tl_currentDimTypeId = -1;
    tl_currentPhase = "idle";
}

// SEH 保护的核心 tick 函数
static TickResult tickDimensionCoreSafe(Dimension* dim, const std::string& initialPhase) {
    TickResult result{0, true, initialPhase};
    
    __try {
        if (dim) {
            dim->tick();
        }
    }
    __except (EXCEPTION_EXECUTE_HANDLER) {
        result.exceptionCode = GetExceptionCode();
        result.success = false;
        // 在任何潜在损坏之前捕获阶段信息
        result.phase = tl_currentPhase;
    }
    
    return result;
}

void ParallelDimensionTickManager::tickDimensionOnWorker(DimensionWorkerContext& ctx) {
    tl_currentPhase = "pre-tick";
    auto start = std::chrono::steady_clock::now();

    tl_currentPhase = "tick";
    TickResult result = tickDimensionCoreSafe(ctx.dimensionPtr, "tick");
    
    if (!result.success) {
        mStats.totalSEHCaught.fetch_add(1, std::memory_order_relaxed);
        
        logger().error("SEH 异常 (代码: 0x{:X}) 发生在维度 {} 的 [{}] 阶段",
            result.exceptionCode, tl_currentDimTypeId, result.phase);
        
        // 只标记特定阶段为危险，不标记通用阶段
        if (result.phase != "pre-tick" && 
            result.phase != "post-tick" && 
            result.phase != "idle" && 
            result.phase != "tick") {
            markFunctionDangerous(result.phase);
        }
        
        ctx.tickFaulted.store(true, std::memory_order_release);
        mFallbackToSerial.store(true, std::memory_order_relaxed);
        
        tl_currentPhase = "idle";
        return;
    }

    tl_currentPhase = "post-tick";
    auto end = std::chrono::steady_clock::now();
    ctx.lastTickTimeUs = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

    uint64_t expected = mStats.maxDimTickTimeUs.load(std::memory_order_relaxed);
    while (ctx.lastTickTimeUs > expected) {
        if (mStats.maxDimTickTimeUs.compare_exchange_weak(expected, ctx.lastTickTimeUs,
            std::memory_order_relaxed, std::memory_order_relaxed)) break;
    }

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

    std::vector<Dimension*> dimRefs;
    {
        std::lock_guard<std::mutex> lock(g_dimensionCollectionMutex);
        dimRefs = g_collectedDimensions;
    }
    
    if (dimRefs.empty()) return;

    std::vector<Dimension*> validDims;
    for (auto* dim : dimRefs) {
        if (dim) {
            validDims.push_back(dim);
        } else {
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
                logger().info("尝试从回退模式恢复，当前 tick {}", currentTick);
            }
            mFallbackToSerial.store(false, std::memory_order_relaxed);
        } else {
            serialFallbackTick(dimRefs);
            return;
        }
    }

    // 确保每个维度有 worker
    for (auto* dim : validDims) {
        int dimId = dim->getDimensionId();
        auto it = mContexts.find(dimId);
        if (it == mContexts.end()) {
            auto ctx = std::make_unique<DimensionWorkerContext>();
            ctx->dimensionPtr = dim;
            ctx->tickCompleted.store(false, std::memory_order_relaxed);
            ctx->isProcessing.store(false, std::memory_order_relaxed);
            ctx->tickFaulted.store(false, std::memory_order_relaxed);
            ctx->shutdown.store(false, std::memory_order_relaxed);
            ctx->shouldWork.store(false, std::memory_order_relaxed);
            ctx->tickNumber.store(0, std::memory_order_relaxed);
            ctx->skippedTicks.store(0, std::memory_order_relaxed);
            ctx->totalSkippedTicks.store(0, std::memory_order_relaxed);
            ctx->workerThread = std::thread(&ParallelDimensionTickManager::workerLoop, this, ctx.get());
            mContexts[dimId] = std::move(ctx);
        } else {
            it->second->dimensionPtr = dim;
        }
    }

    // 异步启动维度 tick
    for (auto* dim : validDims) {
        int dimId = dim->getDimensionId();
        auto& ctx = *mContexts[dimId];

        if (ctx.isProcessing.load(std::memory_order_acquire)) {
            uint64_t skipped = ctx.skippedTicks.fetch_add(1, std::memory_order_relaxed) + 1;
            ctx.totalSkippedTicks.fetch_add(1, std::memory_order_relaxed);
            mStats.totalTicksSkippedDueToBacklog.fetch_add(1, std::memory_order_relaxed);

            if (config.debug && skipped % 20 == 0) {
                logger().warn("维度 {} 已跳过 {} 个连续 tick（总计：{}）",
                    dimId, skipped, ctx.totalSkippedTicks.load(std::memory_order_relaxed));
            }
            continue;
        }

        uint64_t prevSkipped = ctx.skippedTicks.exchange(0, std::memory_order_relaxed);
        if (prevSkipped > 0 && config.debug) {
            logger().info("维度 {} 在跳过 {} 个 tick 后恢复", dimId, prevSkipped);
        }

        ctx.tickCompleted.store(false, std::memory_order_release);
        ctx.tickFaulted.store(false, std::memory_order_release);
        ctx.isProcessing.store(true, std::memory_order_release);
        ctx.tickNumber.fetch_add(1, std::memory_order_relaxed);

        ctx.shouldWork.store(true, std::memory_order_release);
        ctx.wakeCV.notify_one();
    }

    // 非阻塞处理已完成维度的主线程任务
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

            // 如果该维度 tick 出错了，在 fallback 期间用串行补一次
            if (ctx.tickFaulted.load(std::memory_order_acquire)) {
                logger().warn("维度 {} 出错，运行串行恢复 tick", dimId);
                try {
                    dim->tick();
                } catch (...) {
                    logger().error("维度 {} 的恢复 tick 也失败了", dimId);
                }
                ctx.tickFaulted.store(false, std::memory_order_release);
            }
        }
    }

    if (totalTasksThisCycle > 0) {
        mStats.totalMainThreadTasks.fetch_add(totalTasksThisCycle, std::memory_order_relaxed);
        mStats.cycleMainThreadTasks.fetch_add(totalTasksThisCycle, std::memory_order_relaxed);
    }

    if (mFallbackToSerial.load(std::memory_order_relaxed) && mFallbackStartTick == 0) {
        mFallbackStartTick = level->getTime();
    }

    mStats.totalParallelTicks.fetch_add(1, std::memory_order_relaxed);

    if (config.debug && (mStats.totalParallelTicks.load(std::memory_order_relaxed) % 200 == 0)) {
        uint64_t cycleTasks = mStats.cycleMainThreadTasks.exchange(0, std::memory_order_relaxed);
        logger().info(
            "并行 tick #{}: dims={} mainTasks={} skippedTotal={} sehCaught={} fallbacks={} dangerous={} recovery={}",
            mStats.totalParallelTicks.load(std::memory_order_relaxed),
            validDims.size(),
            cycleTasks,
            mStats.totalTicksSkippedDueToBacklog.load(std::memory_order_relaxed),
            mStats.totalSEHCaught.load(std::memory_order_relaxed),
            mStats.totalFallbackTicks.load(std::memory_order_relaxed),
            mStats.totalDangerousFunctions.load(std::memory_order_relaxed),
            mStats.totalRecoveryAttempts.load(std::memory_order_relaxed)
        );
        for (auto* dim : validDims) {
            int dimId = dim->getDimensionId();
            auto& ctx = *mContexts[dimId];
            logger().info(" dim[{}]: {}us (tick #{}, skipped: {})",
                dimId, ctx.lastTickTimeUs,
                ctx.tickNumber.load(std::memory_order_relaxed),
                ctx.totalSkippedTicks.load(std::memory_order_relaxed));
        }

        // 打印当前已知的危险函数
        std::lock_guard lock(m_dangerousMutex);
        if (!m_dangerousFunctions.empty()) {
            std::string funcs;
            for (auto& f : m_dangerousFunctions) {
                if (!funcs.empty()) funcs += ", ";
                funcs += f;
            }
            logger().info(" 危险函数: [{}]", funcs);
        }
    }
}

void ParallelDimensionTickManager::serialFallbackTick(const std::vector<Dimension*>& dimRefs) {
    for (auto* dim : dimRefs) {
        if (dim) {
            try {
                dim->tick();
            } catch (...) {
                logger().error("串行 tick 维度 {} 时发生异常", dim->getDimensionId());
            }
        }
    }
    mStats.totalFallbackTicks.fetch_add(1, std::memory_order_relaxed);
}

//=============================================================================
// Hook helper - 自动适应
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

    {
        std::lock_guard<std::mutex> lock(g_dimensionCollectionMutex);
        g_collectedDimensions.clear();
    }
    
    g_suppressDimensionTick.store(true, std::memory_order_release);

    this->forEachDimension([&](Dimension& dim) -> bool {
        std::lock_guard<std::mutex> lock(g_dimensionCollectionMutex);
        g_collectedDimensions.emplace_back(&dim);
        return true;
    });

    origin();

    g_suppressDimensionTick.store(false, std::memory_order_release);

    bool hasDimensions = false;
    {
        std::lock_guard<std::mutex> lock(g_dimensionCollectionMutex);
        hasDimensions = !g_collectedDimensions.empty();
    }
    
    if (hasDimensions) {
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
    handleDangerousFunction(funcName, [this, &packet, except]() {
        origin(packet, except);
    });
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
    handleDangerousFunction(funcName, [this, &position, &packet, except]() {
        origin(position, packet, except);
    });
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
    handleDangerousFunction(funcName, [this, &actor, &packet, except]() {
        origin(actor, packet, except);
    });
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
        logger().warn("加载配置失败，使用默认配置");
        saveConfig();
    }
    logger().info("DimParallel 已加载。enabled={} debug={}", config.enabled, config.debug);
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
    logger().info("DimParallel 已启用");
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
    logger().info("DimParallel 已禁用");
    return true;
}

} // namespace dim_parallel

LL_REGISTER_MOD(dim_parallel::PluginImpl, dim_parallel::PluginImpl::getInstance());

