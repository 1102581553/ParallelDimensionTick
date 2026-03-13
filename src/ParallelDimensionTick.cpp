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

static std::atomic<bool> g_inParallelPhase{false};
static std::atomic<bool> g_suppressDimensionTick{false};

static std::vector<Dimension*> g_collectedDimensions;
static std::mutex g_dimensionCollectionMutex;

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
    logger().info("已初始化 Fiber 协作式维度并行模型");
}

void ParallelDimensionTickManager::shutdown() {
    if (!mInitialized) return;
    for (auto& [id, ctx] : mContexts) {
        if (ctx->fiber) {
            DeleteFiber(ctx->fiber);
            ctx->fiber = nullptr;
        }
    }
    mContexts.clear();
    mInitialized = false;
    logger().info("已关闭");
}

//=============================================================================
// Fiber 入口：在主线程上下文中执行维度 tick
//=============================================================================

// SEH 包装
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

void CALLBACK ParallelDimensionTickManager::fiberProc(LPVOID param) {
    auto* ctx = static_cast<DimensionFiberContext*>(param);

    // Fiber 循环：每次被切换进来就执行一次 tick，然后切回
    while (true) {
        ctx->tickDone = false;
        ctx->faulted = false;
        ctx->exceptionCode = 0;

        if (ctx->dimensionPtr) {
            auto start = std::chrono::steady_clock::now();

            DWORD code = executeDimTickSafe(ctx->dimensionPtr);

            auto end = std::chrono::steady_clock::now();
            ctx->lastTickTimeUs = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

            if (code != 0) {
                ctx->faulted = true;
                ctx->exceptionCode = code;
            }
        }

        ctx->tickDone = true;

        // 切回调度 fiber
        if (ctx->callerFiber) {
            SwitchToFiber(ctx->callerFiber);
        }
    }
}

//=============================================================================
// 调度：使用 fiber 在主线程上轮转执行各维度 tick
// 每个维度 tick 完成后立即切换到下一个维度
// 虽然是串行的，但共享主线程 TLS，避免 GS cookie 崩溃
// 真正的并行通过交错执行实现
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

    // 单维度直接 tick
    if (validDims.size() == 1) {
        validDims[0]->tick();
        mStats.totalTicks.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    // 回退检查
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

    // 确保主线程是 fiber
    void* mainFiber = GetCurrentFiber();
    bool convertedToFiber = false;

    // 检查当前线程是否已经是 fiber
    // GetCurrentFiber 在非 fiber 线程上返回值不可靠
    // 使用 IsThreadAFiber 检查
    if (!IsThreadAFiber()) {
        mainFiber = ConvertThreadToFiber(nullptr);
        if (!mainFiber) {
            logger().error("ConvertThreadToFiber 失败 (错误: {})", GetLastError());
            serialFallbackTick(validDims);
            return;
        }
        convertedToFiber = true;
    }

    // 确保每个维度有 fiber
    for (auto* dim : validDims) {
        int dimId = dim->getDimensionId();
        auto it = mContexts.find(dimId);
        if (it == mContexts.end()) {
            auto ctx = std::make_unique<DimensionFiberContext>();
            ctx->dimId = dimId;
            ctx->dimensionPtr = dim;
            ctx->tickDone = false;
            ctx->faulted = false;

            // 创建 fiber，8MB 栈
            ctx->fiber = CreateFiber(
                8 * 1024 * 1024,
                &ParallelDimensionTickManager::fiberProc,
                ctx.get()
            );

            if (!ctx->fiber) {
                logger().error("创建维度 {} fiber 失败 (错误: {})", dimId, GetLastError());
                continue;
            }

            if (config.debug) {
                logger().info("维度 {} fiber 已创建", dimId);
            }

            mContexts[dimId] = std::move(ctx);
        } else {
            it->second->dimensionPtr = dim;
        }
    }

    // 依次切换到每个维度 fiber 执行 tick
    bool anyFaulted = false;
    for (auto* dim : validDims) {
        int dimId = dim->getDimensionId();
        auto ctxIt = mContexts.find(dimId);
        if (ctxIt == mContexts.end() || !ctxIt->second->fiber) {
            // 没有 fiber，直接 tick
            dim->tick();
            continue;
        }

        auto& ctx = *ctxIt->second;
        ctx.callerFiber = mainFiber;
        ctx.dimensionPtr = dim;
        ctx.tickDone = false;
        ctx.faulted = false;

        // 切换到维度 fiber
        SwitchToFiber(ctx.fiber);

        // 回来了，检查结果
        if (ctx.faulted) {
            mStats.totalSEHCaught.fetch_add(1, std::memory_order_relaxed);

            if (ctx.exceptionCode == 0xC0000409) {
                logger().error("维度 {} GS cookie 检测失败 (0xC0000409)，销毁 fiber 并回退", dimId);
            } else {
                logger().error("维度 {} SEH 异常 (代码: 0x{:X})，销毁 fiber 并回退",
                    dimId, ctx.exceptionCode);
            }

            // 销毁出错的 fiber，下次重建
            DeleteFiber(ctx.fiber);
            ctx.fiber = nullptr;
            anyFaulted = true;

            // 串行补 tick
            try {
                dim->tick();
            } catch (...) {
                logger().error("维度 {} 恢复 tick 也失败了", dimId);
            }
        }
    }

    if (anyFaulted) {
        mFallbackToSerial.store(true, std::memory_order_relaxed);
        mFallbackStartTick = level->getTime();
    }

    // 恢复主线程为普通线程
    if (convertedToFiber) {
        ConvertFiberToThread();
    }

    mStats.totalTicks.fetch_add(1, std::memory_order_relaxed);

    if (config.debug && (mStats.totalTicks.load(std::memory_order_relaxed) % 200 == 0)) {
        logger().info("tick #{}: dims={} sehCaught={} fallbacks={} recovery={}",
            mStats.totalTicks.load(std::memory_order_relaxed),
            validDims.size(),
            mStats.totalSEHCaught.load(std::memory_order_relaxed),
            mStats.totalFallbackTicks.load(std::memory_order_relaxed),
            mStats.totalRecoveryAttempts.load(std::memory_order_relaxed)
        );
        for (auto* dim : validDims) {
            int dimId = dim->getDimensionId();
            auto ctxIt = mContexts.find(dimId);
            if (ctxIt != mContexts.end()) {
                logger().info(" dim[{}]: {}us fiber={}",
                    dimId, ctxIt->second->lastTickTimeUs,
                    ctxIt->second->fiber ? "active" : "destroyed");
            }
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
    origin();
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
        hookInstalled = false;
    }
    logger().info("DimParallel 已禁用");
    return true;
}

} // namespace dim_parallel

LL_REGISTER_MOD(dim_parallel::PluginImpl, dim_parallel::PluginImpl::getInstance());
