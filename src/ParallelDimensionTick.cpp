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
   }

size_t MainThreadTaskQueue::size() const {
    std::lock_guard lock(mMutex);
    return mTasks.size();
}

ParallelDimensionTickManager& ParallelDimensionTickManager::getInstance() {
    static ParallelDimensionTickManager instance;
    return instance;
}

void ParallelDimensionTickManager::initialize() {
    if (mInitialized) return;
    mFallbackToSerial = false;
    mLastFallbackGameTime = 0;
    mLastSuccessfulParallelTick = 0;
    
    mInitialized = true;
    
    if (config.debug) {
        logger().info("ParallelDimensionTickManager initialized with complete isolation");
    }
}

void ParallelDimensionTickManager::shutdown() {
    if (!mInitialized) return;
    
    // 关闭所有维度线程
    for (auto& [id, ctx] : mContexts) {
        ctx->shutdown();
    }
    mContexts.clear();
    mInitialized = false;
    
    if (config.debug) {
        logger().info("ParallelDimensionTickManager shutdown");
    }
}

bool ParallelDimensionTickManager::isWorkerThread() { return tl_isWorkerThread; }
DimensionWorkerContext* ParallelDimensionTickManager::getCurrentContext() { return tl_currentContext; }
int ParallelDimensionTickManager::getCurrentDimensionType() { 
    return tl_currentDimTypeId; 
}

void ParallelDimensionTickManager::runOnMainThread(std::function<void()> task) {
    if (!tl_isWorkerThread || !tl_currentContext) {
        task();
        return;
    }
    tl_currentContext->mainThreadTasks.enqueue(std::move(task));
}

bool ParallelDimensionTickManager::shouldRecoverFromFallback() {
   ToSerial.load(std::memory_order_acquire)) {
        return false;
    }
    
    // 检查是否过了恢复延迟时间
    int64_t currentTime = mSnapshot.time;
    int64_t timeSinceFallback = currentTime - mLastFallbackGameTime.load(std::memory_order_relaxed);
    
    if (timeSinceFallback >= RECOVERY_DELAY) {
        logger().info("Attempting to recover from fallback mode");
        return true;
    }
    
    return false;
}

void ParallelDimensionTickManager::dispatchAndSync(Level* level, std::vector<Dimension*>& dimensions) {
    if (!level || !mInitialized) {
        serialFallbackTick(dimensions);
        return;
    }

    mSnapshot.time      = level->getTime();
    mSnapshot.simPaused = level->getSimPaused();
    
    if (config.debug) {
        logger().info("dispatchAndSync: time={}, simPaused={}, dims={}", 
            mSnapshot.time, mSnapshot.simPaused, dimensions.size());
    }
    
    if (mSnapshot.simPaused) return;

    if (dimensions.empty()) return;

    // 检查是否需要尝试恢复
    if (mFallbackToSerial.load(std::memory_order_acquire) && shouldRecoverFromFallback()) {
        // 尝试恢复到并行模式
        mFallbackToSerial.store(false, std::memory_order_release);
        mLastSuccessfulParallelTick.store(mSnapshot.time, std::memory_order_relaxed);
    }

    // 如果仍处于降级模式，执行串行回退
    if (mFallbackToSerial.load(std::memory_order_acquire)) {
        serialFallbackTick(dimensions);
        return;
    }

    // 准备维度上下文和线程
    for (auto* dim : dimensions) {
        int dimId = static_cast<int>(dim->getDimensionId());
        auto& ctx = mContexts[dimId];
        if (!ctx) {
            ctx = std::make_unique<DimensionWorkerContext>();
            ctx->dimension = dim;
            ctx->startDimensionThread();
        } else {
            ctx->dimension = dim;
        }
    }

    // 添加tick任务到各维度的专属线程
    std::atomic<int> pendingDimensions{0};
    pendingDimensions.store(static_cast<int>(dimensions.size()), std::memory_order_release);

    for (auto* dim : dimensions) {
        int dimId = static_cast<int>(dim->getDimensionId());
        auto& ctx = mContexts[dimId];
        
        // 添加tick任务到该维度的专属线程
        ctx->addTickTask([&pendingDimensions, this, &ctx]() {
            tickDimensionAsync(ctx);
            // 任务完成后减少计数
            pendingDimensions.fetch_sub(1, std::memory_order_release);
        });
    }

    if (config.debug) {
        logger().info("Added tick tasks to {} dimension threads", dimensions.size());
    }

    // 等待所有维度线程完成任务，同时处理主线程任务以避免死锁
    while (pendingDimensions.load(std::memory_order_acquire) > 0) {
        processAllMainThreadTasks();
        std::this_thread::sleep_for(std::chrono::microseconds(10)); // 避免CPU空转
    }
    
    if (config.debug) {
        logger().info("All dimension tasks completed");
    }

    // 最后处理剩余的主线程任务
    processAllMainThreadTasks();

    mStats.totalParallelTicks++;
    mLastSuccessfulParallelTick.store(mSnapshot.time, std::memory_order_relaxed);

    if (config.debug && (mStats.totalParallelTicks % 50 == 0)) {
        logger().info(
            "Stats: parallelTicks={} fallbackTicks={} mainDimTick={}us",
            mStats.totalParallelTicks.load(),
            mStats.totalFallbackTicks.load(),
            mStats.totalMainThreadTasks.load(),
            mStats.maxDimTickTimeUs.load()
        );
    }
}

void ParallelDimensionTickManager::tickDimensionAsync(DimensionWorkerContext& ctx) {
    // 设置线程局部变量
    tl_isWorkerThread = true;
    tl_currentContext = &ctx;
    tl_currentDimTypeId = ctx.dimension ? static_cast<int>(ctx.dimension->getDimensionId()) : -1;
    tl_currentPhase = "pre-tick";

    auto start = std::chrono::steady_clock::now();

    if (config.debug) {
        logger().info("Starting tick for dimension {}", tl_currentDimTypeId);
    }

    try {
        tl_currentPhase = "tick";
        ctx.dimension->tick();
        tl_currentPhase = "post-tick";
    } catch (std::exception& e) {
        logger().error("Exception in dim {} during [{}]: {}", 
            tl_currentDimTypeId, tl_currentPhase, e.what());
        handleTickException(tl_currentDimTypeId);
    } catch (...) {
        logger().error("SEH/unknown exception in dim {} during [{}]", 
            tl_currentDimTypeId, tl_currentPhase);
        handleTickException(tl_currentDimTypeId);
    }

    auto end = std::chrono::steady_clock::now();
    ctx.lastTickTimeUs = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

    uint64_t expected = mStats.maxDimTickTimeUs.load(std::memory_order_relaxed);
    while (ctx.lastTickTimeUs > expected) {
        if (mStats.maxDimTickTimeUs.compare_exchange_weak(expected, ctx.lastTickTimeUs)) break;
    }

    if (config.debug) {
        logger().info("Finished tick for dimension {} ({}us)", 
            tl_currentDimTypeId, ctx.lastTickTimeUs);
    }

    // 重置线程局部变量
    tl_isWorkerThread = false;
    tl_currentContext = nullptr;
    tl_currentDimTypeId = -1;
    tl_currentPhase = "idle";
}

void ParallelDimensionTickManager::processAllMainThreadTasks() {
    for (auto& [dimId, ctx] : mContexts) {
        size_t count = ctx->mainThreadTasks.size();
        if (count > 0) {
            if (config.debug) {
                logger().info("Processing {} main thread tasks for dimension {}", count, dimId);
            }
            ctx->mainThreadTasks.processAll();
            mStats.totalMainThreadTasks += count;
        }
    }
}

void ParallelDimensionTickManager::serialFallbackTick(std::vector<Dimension*>& dimensions) {
    if (config.debug) {
        logger().info("Falling back to serial tick for {} dimensions", dimensions.size());
    }
    
    for (auto* dim : dimensions) {
        dim->tick();
    }
    mStats.totalFallbackTicks++;
    mLastFallbackGameTime.store(static_cast<int64_t>(mSnapshot.time), std::memory_order_relaxed);
}

void ParallelDimensionTickManager::handleTickException(int dimId) {
    mFallbackToSerial.store(true, std::memory_order_release);
    mLastFallbackGameTime.store(mSnapshot.time, std::memory_order_relaxed);
    mStats.totalFallbackTicks.fetch_add(1, std::memory_order_relaxed);
    
    logger().warn("Fallback to serial ticking due to exception in dim {}", dimId);
}

} // namespace dim_parallel

using namespace dim_parallel;

// Hooks for entity operations that may cause SEH
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
    
    if (config.debug) {
        logger().info("Hook: _processEntityChunkTransfers in dim {}", 
            ParallelDimensionTickManager::getCurrentDimensionType());
    }
    
    tl_currentPhase = "_processEntityChunkTransfers";
    try {
        origin();
    } catch (...) {
        logger().error("Exception in dim {} during _processEntityChunkTransfers", 
            ParallelDimensionTickManager::getCurrentDimensionType());
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
    
    if (config.debug) {
        logger().info("Hook: _tickEntityChunkMoves in dim {}", 
            ParallelDimensionTickManager::getCurrentDimensionType());
    }
    
    tl_currentPhase = "_tickEntityChunkMoves";
    try {
        origin();
    } catch (...) {
        logger().error("Exception in dim {} during _tickEntityChunkMoves", 
            ParallelDimensionTickManager::getCurrentDimensionType());
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
        if (config.debug) {
            logger().info("Collecting dimension {} for parallel tick", static_cast<int>(this->getDimensionId()));
        }
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

    g_collectedDimensions.clear();
    g_suppressDimensionTick.store(true, std::memory_order_release);

    if (config.debug) {
        logger().info("Level tick started, collecting dimensions");
    }

    origin();

    g_suppressDimensionTick.store(false, std::memory_order_release);

    if (!g_collectedDimensions.empty()) {
        g_inParallelPhase.store(true, std::memory_order_release);
        
        if (config.debug) {
            logger().info("Dispatching {} dimensions for parallel tick", g_collectedDimensions.size());
        }
        
        auto dims = g_collectedDimensions;
        ParallelDimensionTickManager::getInstance().dispatchAndSync(this, dims);
        
        g_inParallelPhase.store(false, std::memory_order_release);
        
        if (config.debug) {
            logger().info("Parallel tick completed");
        }
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
        DimensionProcessEntityTransfersHook::hook();
        DimensionTickEntityChunkMovesHook::hook();
        DimensionRunChunkGenWatchdogHook::hook();
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
        DimensionProcessEntityTransfersHook::unhook();
        DimensionTickEntityChunkMovesHook::unhook();
        DimensionRunChunkGenWatchdogHook::unhook();
        hookInstalled = false;
    }
    logger().info("DimParallel disabled");
    return true;
}

} // namespace dim_parallel

LL_REGISTER_MOD(dim_parallel::PluginImpl, dim_parallelgetInstance());
