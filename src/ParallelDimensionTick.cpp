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
#include <cstring>

namespace dim_parallel {

static Config config;
static std::shared_ptr<ll::io::Logger> log;
static bool hookInstalled = false;

static thread_local DimensionFiberContext* tl_currentContext = nullptr;
static thread_local bool tl_isWorkerThread = false;
static thread_local int tl_currentDimTypeId = -1;
static thread_local char tl_currentPhase[64] = "idle";

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

    SYSTEM_INFO sysInfo;
    GetSystemInfo(&sysInfo);
    mWorkerCount = static_cast<int>(sysInfo.dwNumberOfProcessors) - 1;
    if (mWorkerCount < 1) mWorkerCount = 1;

    mInitialized = true;
    logger().info("已初始化 Fiber-per-Thread 并行模型（CPU: {}, 工作线程: {}）",
        sysInfo.dwNumberOfProcessors, mWorkerCount);
}

void ParallelDimensionTickManager::shutdown() {
    if (!mInitialized) return;
    for (auto& [id, ctx] : mContexts) {
        ctx->shutdown.store(true, std::memory_order_release);
        ctx->wakeCV.notify_one();
        if (ctx->workerThread) {
            WaitForSingleObject(ctx->workerThread, 5000);
            CloseHandle(ctx->workerThread);
            ctx->workerThread = nullptr;
        }
        // fiber 由工作线程创建和销毁，这里不需要处理
    }
    mContexts.clear();
    mInitialized = false;
    logger().info("已关闭");
}

bool ParallelDimensionTickManager::isWorkerThread() { return tl_isWorkerThread; }

DimensionFiberContext* ParallelDimensionTickManager::getCurrentContext() { return tl_currentContext; }

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

//=============================================================================
// SEH 包装
//=============================================================================

static DWORD executeDimTickSafe(Dimension* dim) {
    __try {
        if (dim) {
            dim->tick();
        }
        return 0;
    }
    __except (EXCEPTION_EXECUTE_HANDLER) {
        return GetExceptionCode();
    }
}

//=============================================================================
// 维度 Fiber 入口
//=============================================================================

void CALLBACK ParallelDimensionTickManager::dimFiberProc(LPVOID param) {
    auto* ctx = static_cast<DimensionFiberContext*>(param);

    // fiber 循环：每次被切换进来执行一次 tick，然后切回工作线程 fiber
    while (true) {
        ctx->tickDone = false;
        ctx->faulted = false;
        ctx->exceptionCode = 0;

        if (ctx->dimensionPtr) {
            auto start = std::chrono::steady_clock::now();

            DWORD code = executeDimTickSafe(ctx->dimensionPtr);

            auto end = std::chrono::steady_clock::now();
            ctx->lastTickTimeUs =
                std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

            if (code != 0) {
                ctx->faulted = true;
                ctx->exceptionCode = code;
            }
        }

        ctx->tickDone = true;

        // 切回工作线程的主 fiber
        // 工作线程在 ConvertThreadToFiber 后得到的 fiber
        void* workerFiber = GetCurrentFiber();
        // 不能切回自己，需要一个外部存储的 caller fiber
        // 我们用 tl_currentContext 中不存在的字段...
        // 实际上 fiber 切换需要知道切回哪里
        // 解决方案：在 ctx 中存储 workerMainFiber
        // 但这里 dimFiberProc 是 fiber 入口，第一次进来时还没有设置
        // 所以我们需要在 workerThreadProc 中设置后再切换

        // 这里直接用 SwitchToFiber 切回，workerMainFiber 在 ctx 中
        // 需要在 header 中添加字段... 但为了不改 header，用 thread_local
        // 不行，fiber 不等于 thread_local
        // 最简单：在 DimensionFiberContext 中加一个 callerFiber 字段
        // header 中已经没有了，需要加回来

        // 暂时用一个全局 thread_local
        // fiber 运行在哪个线程上，thread_local 就是那个线程的
        // 这是安全的
        extern thread_local void* tl_workerMainFiber;
        if (tl_workerMainFiber) {
            SwitchToFiber(tl_workerMainFiber);
        }
        // 如果切回失败，fiber 会在这里死循环等待下次切入
    }
}

//=============================================================================
// 工作线程：每个维度一个线程，线程内用 fiber 执行 tick
//=============================================================================

thread_local void* tl_workerMainFiber = nullptr;

DWORD WINAPI ParallelDimensionTickManager::workerThreadProc(LPVOID param) {
    auto* ctx = static_cast<DimensionFiberContext*>(param);

    tl_isWorkerThread = true;
    tl_currentContext = ctx;
    tl_currentDimTypeId = ctx->dimId;

    // 将工作线程转换为 fiber
    tl_workerMainFiber = ConvertThreadToFiber(nullptr);
    if (!tl_workerMainFiber) {
        logger().error("维度 {} 工作线程 ConvertThreadToFiber 失败 (错误: {})",
            ctx->dimId, GetLastError());
        return 1;
    }

    // 创建维度 tick fiber（8MB 栈）
    ctx->dimFiber = CreateFiber(
        8 * 1024 * 1024,
        &ParallelDimensionTickManager::dimFiberProc,
        ctx
    );

    if (!ctx->dimFiber) {
        logger().error("维度 {} CreateFiber 失败 (错误: {})", ctx->dimId, GetLastError());
        ConvertFiberToThread();
        return 1;
    }

    logger().info("维度 {} fiber 工作线程就绪", ctx->dimId);

    while (true) {
        // 等待调度信号
        {
            std::unique_lock lock(ctx->wakeMutex);
            ctx->wakeCV.wait(lock, [ctx] {
                return ctx->shouldWork.load(std::memory_order_acquire) ||
                       ctx->shutdown.load(std::memory_order_acquire);
            });
        }

        if (ctx->shutdown.load(std::memory_order_acquire)) break;

        ctx->shouldWork.store(false, std::memory_order_release);

        std::atomic_thread_fence(std::memory_order_acquire);

        if (ctx->dimensionPtr && ctx->dimFiber) {
            tl_currentDimTypeId = ctx->dimensionPtr->getDimensionId();
            strncpy_s(tl_currentPhase, "tick", _TRUNCATE);

            // 切换到维度 fiber 执行 tick
            SwitchToFiber(ctx->dimFiber);

            // tick 完成，回到这里
            strncpy_s(tl_currentPhase, "idle", _TRUNCATE);

            if (ctx->faulted) {
                getInstance().mStats.totalSEHCaught.fetch_add(1, std::memory_order_relaxed);

                if (ctx->exceptionCode == 0xC0000409) {
                    logger().error("维度 {} GS cookie 失败，销毁 fiber", ctx->dimId);
                } else if (ctx->exceptionCode == 0xC0000005) {
                    logger().error("维度 {} 访问违规 (0x{:X})", ctx->dimId, ctx->exceptionCode);
                } else {
                    logger().error("维度 {} SEH 异常 (0x{:X})", ctx->dimId, ctx->exceptionCode);
                }

                // 销毁出错的 fiber，下次重建
                DeleteFiber(ctx->dimFiber);
                ctx->dimFiber = CreateFiber(
                    8 * 1024 * 1024,
                    &ParallelDimensionTickManager::dimFiberProc,
                    ctx
                );

                getInstance().mFallbackToSerial.store(true, std::memory_order_relaxed);
            }
        }

        ctx->isProcessing.store(false, std::memory_order_release);
        ctx->tickCompleted.store(true, std::memory_order_release);
    }

    // 清理
    if (ctx->dimFiber) {
        DeleteFiber(ctx->dimFiber);
        ctx->dimFiber = nullptr;
    }
    ConvertFiberToThread();

    tl_isWorkerThread = false;
    tl_currentContext = nullptr;
    tl_workerMainFiber = nullptr;
    return 0;
}

//=============================================================================
// 调度
//=============================================================================

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

    if (level->getSimPaused()) return;

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
        mStats.totalParallelTicks.fetch_add(1, std::memory_order_relaxed);
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
            serialFallbackTick(validDims);
            return;
        }
    }

    // 确保每个维度有工作线程
    for (auto* dim : validDims) {
        int dimId = dim->getDimensionId();
        auto it = mContexts.find(dimId);
        if (it == mContexts.end()) {
            auto ctx = std::make_unique<DimensionFiberContext>();
            ctx->dimId = dimId;
            ctx->dimensionPtr = nullptr;
            ctx->tickCompleted.store(false, std::memory_order_relaxed);
            ctx->isProcessing.store(false, std::memory_order_relaxed);
            ctx->shutdown.store(false, std::memory_order_relaxed);
            ctx->shouldWork.store(false, std::memory_order_relaxed);
            ctx->tickNumber.store(0, std::memory_order_relaxed);
            ctx->skippedTicks.store(0, std::memory_order_relaxed);
            ctx->totalSkippedTicks.store(0, std::memory_order_relaxed);

            ctx->workerThread = CreateThread(
                nullptr,
                0, // 默认栈，fiber 有自己的栈
                &ParallelDimensionTickManager::workerThreadProc,
                ctx.get(),
                0,
                nullptr
            );

            if (!ctx->workerThread) {
                logger().error("创建维度 {} 工作线程失败 (错误: {})", dimId, GetLastError());
                continue;
            }

            mContexts[dimId] = std::move(ctx);
        }
    }

    // 异步启动维度 tick
    for (auto* dim : validDims) {
        int dimId = dim->getDimensionId();
        auto ctxIt = mContexts.find(dimId);
        if (ctxIt == mContexts.end()) continue;
        auto& ctx = *ctxIt->second;

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

        ctx.dimensionPtr = dim;
        std::atomic_thread_fence(std::memory_order_release);

        ctx.tickCompleted.store(false, std::memory_order_release);
        ctx.isProcessing.store(true, std::memory_order_release);
        ctx.tickNumber.fetch_add(1, std::memory_order_relaxed);

        ctx.shouldWork.store(true, std::memory_order_release);
        ctx.wakeCV.notify_one();
    }

    // 非阻塞处理已完成维度的主线程任务
    uint64_t totalTasksThisCycle = 0;
    for (auto* dim : validDims) {
        int dimId = dim->getDimensionId();
        auto ctxIt = mContexts.find(dimId);
        if (ctxIt == mContexts.end()) continue;
        auto& ctx = *ctxIt->second;

        if (ctx.tickCompleted.load(std::memory_order_acquire)) {
            size_t taskCount = ctx.mainThreadTasks.size();
            if (taskCount > 0) {
                ctx.mainThreadTasks.processAll();
                totalTasksThisCycle += taskCount;
            }
            ctx.tickCompleted.store(false, std::memory_order_release);

            if (ctx.faulted) {
                logger().warn("维度 {} 出错，运行串行恢复 tick", dimId);
                try {
                    dim->tick();
                } catch (...) {
                    logger().error("维度 {} 恢复 tick 也失败了", dimId);
                }
                ctx.faulted = false;
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
            "并行 tick #{}: dims={} mainTasks={} skippedTotal={} sehCaught={} fallbacks={} recovery={}",
            mStats.totalParallelTicks.load(std::memory_order_relaxed),
            validDims.size(),
            cycleTasks,
            mStats.totalTicksSkippedDueToBacklog.load(std::memory_order_relaxed),
            mStats.totalSEHCaught.load(std::memory_order_relaxed),
            mStats.totalFallbackTicks.load(std::memory_order_relaxed),
            mStats.totalRecoveryAttempts.load(std::memory_order_relaxed)
        );
        for (auto* dim : validDims) {
            int dimId = dim->getDimensionId();
            auto ctxIt = mContexts.find(dimId);
            if (ctxIt == mContexts.end()) continue;
            auto& ctx = *ctxIt->second;
            logger().info(" dim[{}]: {}us (tick #{}, skipped: {}, fiber: {})",
                dimId, ctx.lastTickTimeUs,
                ctx.tickNumber.load(std::memory_order_relaxed),
                ctx.totalSkippedTicks.load(std::memory_order_relaxed),
                ctx.dimFiber ? "ok" : "destroyed");
        }

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
                logger().error("串行 tick 维度 {} 时发生异常", static_cast<int>(dim->getDimensionId()));
            }
        }
    }
    mStats.totalFallbackTicks.fetch_add(1, std::memory_order_relaxed);
}

//=============================================================================
// Hook helper
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
    strncpy_s(tl_currentPhase, funcName, _TRUNCATE);
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
    strncpy_s(tl_currentPhase, funcName, _TRUNCATE);
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
    strncpy_s(tl_currentPhase, funcName, _TRUNCATE);
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
    strncpy_s(tl_currentPhase, funcName, _TRUNCATE);
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
    strncpy_s(tl_currentPhase, funcName, _TRUNCATE);
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
    strncpy_s(tl_currentPhase, funcName, _TRUNCATE);
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
    strncpy_s(tl_currentPhase, funcName, _TRUNCATE);
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
    strncpy_s(tl_currentPhase, funcName, _TRUNCATE);
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
