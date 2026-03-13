#include "ParallelDimensionTick.h"

#include <ll/api/memory/Hook.h>
#include <ll/api/mod/RegisterHelper.h>
#include <ll/api/io/LoggerRegistry.h>

#include <mc/world/level/Level.h>
#include <mc/server/ServerLevel.h>
#include <mc/world/level/dimension/Dimension.h>
#include <mc/world/actor/Actor.h>
#include <mc/network/Packet.h>
#include <mc/network/PacketSender.h>
#include <mc/network/LoopbackPacketSender.h>

#include <chrono>
#include <filesystem>
#include <thread>

namespace dim_parallel {

// ==================== 全局状态 ====================

static Config                          config;
static std::shared_ptr<ll::io::Logger> log;
static bool                            hookInstalled = false;

static thread_local DimensionWorkerContext* tl_currentContext   = nullptr;
static thread_local bool                   tl_isWorkerThread   = false;
static thread_local int                    tl_currentDimTypeId = -1;

// Hook 协调标志
static std::atomic<bool>       g_inParallelPhase{false};
static std::atomic<bool>       g_suppressDimensionTick{false};
static std::vector<Dimension*> g_collectedDimensions;
static std::mutex              g_collectMutex;

// ==================== 配置与日志 ====================

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

// ==================== MainThreadTaskQueue ====================

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

// ==================== ParallelDimensionTickManager ====================

ParallelDimensionTickManager& ParallelDimensionTickManager::getInstance() {
    static ParallelDimensionTickManager instance;
    return instance;
}

void ParallelDimensionTickManager::initialize() {
    if (mInitialized) return;
    mFallbackToSerial  = false;
    mShutdownRequested = false;
    mInitialized       = true;
    logger().info("ParallelDimensionTickManager initialized");
}

void ParallelDimensionTickManager::shutdown() {
    if (!mInitialized) return;
    mShutdownRequested = true;
    mContexts.clear();
    mInitialized = false;
    logger().info("ParallelDimensionTickManager shutdown");
}

bool ParallelDimensionTickManager::isWorkerThread() { return tl_isWorkerThread; }

DimensionWorkerContext* ParallelDimensionTickManager::getCurrentContext() { return tl_currentContext; }

DimensionType ParallelDimensionTickManager::getCurrentDimensionType() {
    return DimensionType(tl_currentDimTypeId);
}

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

    // 快照
    mSnapshot.time        = level->getTime();
    mSnapshot.currentTick = level->getCurrentTick().tickID;
    mSnapshot.simPaused   = level->getSimPaused();

    if (mSnapshot.simPaused) return;

    // 收集维度
    std::vector<Dimension*> dimensions;
    level->forEachDimension([&](Dimension& dim) -> bool {
        dimensions.push_back(&dim);
        return true;
    });

    if (dimensions.empty()) return;

    if (dimensions.size() == 1) {
        dimensions[0]->tick();
        return;
    }

    // 准备上下文
    for (auto* dim : dimensions) {
        int dimId  = dim->getDimensionId();
        auto& ctx  = mContexts[dimId];
        ctx.dimension = dim;
    }

    // 并行 tick
    std::latch completionLatch(static_cast<ptrdiff_t>(dimensions.size()));

    for (auto* dim : dimensions) {
        int dimId = dim->getDimensionId();
        auto& ctx = mContexts[dimId];

        std::thread([this, &ctx, &completionLatch]() {
            tickDimensionOnWorker(ctx);
            completionLatch.count_down();
        }).detach();
    }

    completionLatch.wait();

    // 同步阶段
    processAllMainThreadTasks();

    mStats.totalParallelTicks++;

    if (config.debug && (mStats.totalParallelTicks % 200 == 0)) {
        logger().info(
            "Parallel tick #{}: dims={}  mainTasks={}  fallbacks={}",
            mStats.totalParallelTicks.load(),
            dimensions.size(),
            mStats.totalMainThreadTasks.load(),
            mStats.totalFallbackTicks.load()
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

    auto start = std::chrono::steady_clock::now();

    try {
        ctx.dimension->tick();
    } catch (std::exception& e) {
        logger().error("Exception in dim {} tick: {}", tl_currentDimTypeId, e.what());
        mFallbackToSerial = true;
    } catch (...) {
        logger().error("Unknown exception in dim {} tick", tl_currentDimTypeId);
        mFallbackToSerial = true;
    }

    auto end           = std::chrono::steady_clock::now();
    ctx.lastTickTimeUs = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

    uint64_t expected = mStats.maxDimTickTimeUs.load();
    while (ctx.lastTickTimeUs > expected) {
        if (mStats.maxDimTickTimeUs.compare_exchange_weak(expected, ctx.lastTickTimeUs)) break;
    }

    tl_isWorkerThread  = false;
    tl_currentContext   = nullptr;
    tl_currentDimTypeId = -1;
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

// ==================== Hooks ====================

using namespace dim_parallel;

// Hook Dimension::tick() — suppress 模式下只收集指针，parallel 模式下正常执行
LL_TYPE_INSTANCE_HOOK(
    DimensionTickHook,
    ll::memory::HookPriority::Normal,
    Dimension,
    &Dimension::tick,
    void
) {
    if (!config.enabled) {
        origin();
        return;
    }

    if (g_suppressDimensionTick.load(std::memory_order_acquire)) {
        // Level::tick() 内部的 forEachDimension 调用，只收集不执行
        std::lock_guard lock(g_collectMutex);
        g_collectedDimensions.push_back(this);
        return;
    }

    if (g_inParallelPhase.load(std::memory_order_acquire)) {
        // 工作线程调用，正常执行原始 tick
        origin();
        return;
    }

    // 其他路径（不应该发生），安全执行
    origin();
}

// Hook Dimension::sendBroadcast() — 工作线程中延迟到主线程
LL_TYPE_INSTANCE_HOOK(
    DimensionSendBroadcastHook,
    ll::memory::HookPriority::Normal,
    Dimension,
    &Dimension::sendBroadcast,
    void,
    Packet const& packet,
    Player*       except
) {
    if (!config.enabled || !ParallelDimensionTickManager::isWorkerThread()) {
        origin(packet, except);
        return;
    }

    // 在工作线程中，延迟发包到主线程同步阶段
    // 注意：packet 是栈上引用，不能直接捕获引用
    // 使用 Packet::sendTo 系列方法在主线程重放更安全
    // 但这里我们需要调用 origin，所以用 this 指针 + 标志绕过
    Dimension* self = this;
    Player*    exc  = except;

    // 暂时直接调用 origin — Phase 2 会改为真正的缓冲
    // 当前风险：从非主线程调用网络发送
    // TODO: 实现 packet clone 后改为缓冲模式
    origin(packet, except);
}

// Hook Dimension::sendPacketForPosition() — 工作线程中延迟到主线程
LL_TYPE_INSTANCE_HOOK(
    DimensionSendPacketForPositionHook,
    ll::memory::HookPriority::Normal,
    Dimension,
    &Dimension::sendPacketForPosition,
    void,
    BlockPos const& position,
    Packet const&   packet,
    Player const*   except
) {
    if (!config.enabled || !ParallelDimensionTickManager::isWorkerThread()) {
        origin(position, packet, except);
        return;
    }

    // TODO: Phase 2 缓冲
    origin(position, packet, except);
}

// Hook Dimension::sendPacketForEntity() — 工作线程中延迟到主线程
LL_TYPE_INSTANCE_HOOK(
    DimensionSendPacketForEntityHook,
    ll::memory::HookPriority::Normal,
    Dimension,
    &Dimension::sendPacketForEntity,
    void,
    Actor const&  actor,
    Packet const& packet,
    Player const* except
) {
    if (!config.enabled || !ParallelDimensionTickManager::isWorkerThread()) {
        origin(actor, packet, except);
        return;
    }

    // TODO: Phase 2 缓冲
    origin(actor, packet, except);
}

// Hook Level::tick() — 拦截 forEachDimension 中的 dim.tick()，替换为并行
LL_TYPE_INSTANCE_HOOK(
    LevelTickHook,
    ll::memory::HookPriority::Normal,
    Level,
    &Level::tick,
    void
) {
    if (!config.enabled) {
        origin();
        return;
    }

    // 阶段 1: suppress dim.tick()，让 origin() 中的 forEachDimension 只收集指针
    g_collectedDimensions.clear();
    g_suppressDimensionTick.store(true, std::memory_order_release);

    // 调用原始 Level::tick()
    // tickEntities(), tickEntitySystems(), _subTick() 等正常执行
    // forEachDimension 中的 dim.tick() 被 DimensionTickHook 拦截，只收集不执行
    origin();

    g_suppressDimensionTick.store(false, std::memory_order_release);

    // 阶段 2: 并行 tick 收集到的维度
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
    logger().info("DimParallel loaded. enabled={} debug={}", config.enabled, config.debug);
    return true;
}

bool PluginImpl::enable() {
    if (!hookInstalled) {
        LevelTickHook::hook();
        DimensionTickHook::hook();
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
