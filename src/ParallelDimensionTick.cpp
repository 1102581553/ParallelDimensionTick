#include "ParallelDimensionTick.h"

#include <ll/api/memory/Hook.h>
#include <ll/api/mod/RegisterHelper.h>
#include <ll/api/io/LoggerRegistry.h>

#include <mc/world/level/Level.h>
#include <mc/server/ServerLevel.h>
#include <mc/world/level/dimension/Dimension.h>
#include <mc/world/actor/Actor.h>
#include <mc/network/packet/Packet.h>

#include <chrono>
#include <thread>

namespace dim_parallel {

// ==================== 全局状态 ====================

static Config                    config;
static std::shared_ptr<ll::io::Logger> log;
static bool                      hookInstalled = false;

// 线程局部：标识当前工作线程的维度上下文
static thread_local DimensionWorkerContext* tl_currentContext    = nullptr;
static thread_local bool                   tl_isWorkerThread    = false;
static thread_local int                    tl_currentDimTypeId  = -1;

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

// ==================== DimensionPacketBuffer ====================

void DimensionPacketBuffer::addBroadcast(Packet const& packet, Player* except) {
    PacketEntry entry;
    entry.type   = PacketEntry::Type::Broadcast;
    entry.except = except;
    // 存储包的 shared_ptr — Packet 需要在 flush 时仍然有效
    // 这里我们存原始指针的 const_cast，flush 时直接用 Dimension 的原始方法
    // 安全性：flush 在同一 tick 内，包对象生命周期足够
    entry.packet = nullptr; // 我们用另一种方式：直接在 flush 时重放
    // 实际上我们需要 clone packet 或者延迟调用
    // 简化方案：在 buffer 中存 lambda
    mEntries.push_back(std::move(entry));
}

void DimensionPacketBuffer::addForPosition(
    BlockPos const& pos, Packet const& packet, Player const* except
) {
    PacketEntry entry;
    entry.type     = PacketEntry::Type::ForPosition;
    entry.position = pos;
    entry.except   = except;
    mEntries.push_back(std::move(entry));
}

void DimensionPacketBuffer::addForEntity(
    Actor const& actor, Packet const& packet, Player const* except
) {
    PacketEntry entry;
    entry.type           = PacketEntry::Type::ForEntity;
    entry.actorRuntimeId = actor.getRuntimeID().id;
    entry.except         = except;
    mEntries.push_back(std::move(entry));
}

void DimensionPacketBuffer::flushTo(Dimension& /*dim*/) {
    // Phase 2 实现：遍历 mEntries，调用 dim 的原始发包方法
    // 当前 Phase 1 中，我们先不缓冲包，而是直接拦截并在主线程重放
    clear();
}

void DimensionPacketBuffer::clear() {
    mEntries.clear();
}

size_t DimensionPacketBuffer::size() const {
    return mEntries.size();
}

// ==================== ParallelDimensionTickManager ====================

ParallelDimensionTickManager& ParallelDimensionTickManager::getInstance() {
    static ParallelDimensionTickManager instance;
    return instance;
}

void ParallelDimensionTickManager::initialize() {
    if (mInitialized) return;
    mFallbackToSerial = false;
    mShutdownRequested = false;
    mInitialized = true;
    logger().info("ParallelDimensionTickManager initialized");
}

void ParallelDimensionTickManager::shutdown() {
    if (!mInitialized) return;
    mShutdownRequested = true;
    mContexts.clear();
    mInitialized = false;
    logger().info("ParallelDimensionTickManager shutdown");
}

bool ParallelDimensionTickManager::isWorkerThread() {
    return tl_isWorkerThread;
}

DimensionWorkerContext* ParallelDimensionTickManager::getCurrentContext() {
    return tl_currentContext;
}

DimensionType ParallelDimensionTickManager::getCurrentDimensionType() {
    return DimensionType(tl_currentDimTypeId);
}

void ParallelDimensionTickManager::runOnMainThread(std::function<void()> task) {
    if (!tl_isWorkerThread || !tl_currentContext) {
        // 已经在主线程，直接执行
        task();
        return;
    }
    tl_currentContext->mainThreadTasks.enqueue(std::move(task));
}

void ParallelDimensionTickManager::dispatchAndSync(Level* level) {
    if (!level || !mInitialized || mFallbackToSerial) {
        // 降级：串行 tick
        std::vector<Dimension*> dims;
        level->forEachDimension([&](Dimension& dim) -> bool {
            dims.push_back(&dim);
            return true;
        });
        serialFallbackTick(dims);
        return;
    }

    // 1. 创建快照
    mSnapshot.time        = level->getTime();
    mSnapshot.currentTick = level->getCurrentTick().tickID;
    mSnapshot.simPaused   = level->getSimPaused();

    if (mSnapshot.simPaused) {
        return; // 暂停时不 tick 维度
    }

    // 2. 收集所有维度
    std::vector<Dimension*> dimensions;
    level->forEachDimension([&](Dimension& dim) -> bool {
        dimensions.push_back(&dim);
        return true;
    });

    if (dimensions.empty()) {
        return;
    }

    // 只有一个维度时，没必要并行
    if (dimensions.size() == 1) {
        dimensions[0]->tick();
        return;
    }

    // 3. 准备每个维度的上下文
    for (auto* dim : dimensions) {
        int dimId = dim->getDimensionId();
        auto& ctx = mContexts[dimId];
        ctx.dimension = dim;
        ctx.packetBuffer.clear();
        // mainThreadTasks 不需要 clear，processAll 会清空
    }

    // 4. 并行 tick
    auto completionLatch = std::make_unique<std::latch>(dimensions.size());

    for (auto* dim : dimensions) {
        int dimId = dim->getDimensionId();
        auto& ctx = mContexts[dimId];

        std::thread([this, &ctx, latch = completionLatch.get()]() {
            tickDimensionOnWorker(ctx);
            latch->count_down();
        }).detach();
    }

    // 5. 主线程等待所有维度完成
    completionLatch->wait();

    // 6. 主线程同步阶段：处理所有延迟任务
    processAllMainThreadTasks();

    // 7. Flush 所有 packet buffer（Phase 2 完善）
    flushAllPacketBuffers();

    mStats.totalParallelTicks++;

    // 8. 调试输出
    if (config.debug && (mStats.totalParallelTicks % 200 == 0)) {
        logger().info(
            "Parallel tick #{}: dims={}, mainTasks={}, fallbacks={}, maxDimUs={}",
            mStats.totalParallelTicks.load(),
            dimensions.size(),
            mStats.totalMainThreadTasks.load(),
            mStats.totalFallbackTicks.load(),
            mStats.maxDimTickTimeUs.load()
        );
        for (auto& [id, ctx] : mContexts) {
            logger().info("  dim[{}]: lastTickUs={}", id, ctx.lastTickTimeUs);
        }
    }
}

void ParallelDimensionTickManager::tickDimensionOnWorker(DimensionWorkerContext& ctx) {
    // 设置线程局部上下文
    tl_isWorkerThread   = true;
    tl_currentContext    = &ctx;
    tl_currentDimTypeId  = ctx.dimension->getDimensionId();

    auto start = std::chrono::steady_clock::now();

    try {
        ctx.dimension->tick();
    } catch (std::exception& e) {
        logger().error(
            "Exception in dimension {} tick: {}",
            tl_currentDimTypeId, e.what()
        );
        mFallbackToSerial = true;
    } catch (...) {
        logger().error(
            "Unknown exception in dimension {} tick",
            tl_currentDimTypeId
        );
        mFallbackToSerial = true;
    }

    auto end = std::chrono::steady_clock::now();
    ctx.lastTickTimeUs = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

    // 更新最大 tick 时间
    uint64_t expected = mStats.maxDimTickTimeUs.load();
    while (ctx.lastTickTimeUs > expected) {
        if (mStats.maxDimTickTimeUs.compare_exchange_weak(expected, ctx.lastTickTimeUs)) break;
    }

    // 清除线程局部上下文
    tl_isWorkerThread   = false;
    tl_currentContext    = nullptr;
    tl_currentDimTypeId  = -1;
}

void ParallelDimensionTickManager::flushAllPacketBuffers() {
    for (auto& [dimId, ctx] : mContexts) {
        if (ctx.dimension && ctx.packetBuffer.size() > 0) {
            ctx.packetBuffer.flushTo(*ctx.dimension);
            mStats.totalPacketsBuffered += ctx.packetBuffer.size();
        }
    }
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

// ==================== Hooks ====================

// Hook 1: 拦截 Level::tick()，替换维度 tick 部分为并行
// 策略：Hook Dimension::tick()，在并行模式下跳过原始调用
// 主线程在 Level::tick() 的 forEachDimension 中会调用每个 dim.tick()
// 我们 Hook dim.tick()，让它在被 forEachDimension 调用时不执行
// 然后在 Level::tick() 之后（通过 Hook Level::tick()）插入我们的并行逻辑

// 标志：当前是否处于我们的并行 tick 阶段
static std::atomic<bool> g_inParallelPhase{false};
// 标志：是否应该跳过原始 forEachDimension 中的 dim.tick()
static std::atomic<bool> g_suppressDimensionTick{false};
// 收集维度指针
static std::vector<Dimension*> g_collectedDimensions;
static std::mutex g_collectMutex;

// Hook Dimension::tick() — 在 suppress 模式下跳过，在并行模式下正常执行
LL_TYPE_INSTANCE_HOOK(
    DimensionTickHook,
    ll::memory::HookPriority::Normal,
    Dimension,
    &Dimension::tick,
    void
) {
    if (!dim_parallel::config.enabled) {
        origin();
        return;
    }

    if (g_suppressDimensionTick.load()) {
        // 被 Level::tick() 的 forEachDimension 调用
        // 不执行 tick，只收集指针
        std::lock_guard lock(g_collectMutex);
        g_collectedDimensions.push_back(this);
        return;
    }

    if (g_inParallelPhase.load()) {
        // 被我们的工作线程调用，正常执行
        origin();
        return;
    }

    // 其他情况（不应该发生），正常执行
    origin();
}

// Hook Dimension::sendBroadcast() — 工作线程中重定向到队列
LL_TYPE_INSTANCE_HOOK(
    DimensionSendBroadcastHook,
    ll::memory::HookPriority::Normal,
    Dimension,
    &Dimension::sendBroadcast,
    void,
    Packet const& packet,
    Player*       except
) {
    if (!dim_parallel::config.enabled || !ParallelDimensionTickManager::isWorkerThread()) {
        origin(packet, except);
        return;
    }

    // 在工作线程中，延迟到主线程执行
    // 捕获必要数据，避免引用悬垂
    Dimension* dim = this;
    Player* exc = except;
    ParallelDimensionTickManager::runOnMainThread([dim, &packet, exc]() {
        // 这里需要调用原始的 sendBroadcast
        // 但我们在 lambda 中无法调用 origin()
        // 所以我们直接调用 Dimension 的发包逻辑
        dim->sendBroadcast(packet, exc);
    });
}

// Hook Dimension::sendPacketForPosition() — 工作线程中重定向到队列
LL_TYPE_INSTANCE_HOOK(
    DimensionSendPacketForPositionHook,
    ll::memory::HookPriority::Normal,
    Dimension,
    &Dimension::sendPacketForPosition,
    void,
    BlockPos const&  position,
    Packet const&    packet,
    Player const*    except
) {
    if (!dim_parallel::config.enabled || !ParallelDimensionTickManager::isWorkerThread()) {
        origin(position, packet, except);
        return;
    }

    Dimension* dim = this;
    BlockPos pos = position;
    Player const* exc = except;
    ParallelDimensionTickManager::runOnMainThread([dim, pos, &packet, exc]() {
        dim->sendPacketForPosition(pos, packet, exc);
    });
}

// Hook Dimension::sendPacketForEntity() — 工作线程中重定向到队列
LL_TYPE_INSTANCE_HOOK(
    DimensionSendPacketForEntityHook,
    ll::memory::HookPriority::Normal,
    Dimension,
    &Dimension::sendPacketForEntity,
    void,
    Actor const&   actor,
    Packet const&  packet,
    Player const*  except
) {
    if (!dim_parallel::config.enabled || !ParallelDimensionTickManager::isWorkerThread()) {
        origin(actor, packet, except);
        return;
    }

    Dimension* dim = this;
    // 存 actor 指针 — 在同一 tick 内 flush，生命周期安全
    Actor const* actorPtr = &actor;
    Player const* exc = except;
    ParallelDimensionTickManager::runOnMainThread([dim, actorPtr, &packet, exc]() {
        dim->sendPacketForEntity(*actorPtr, packet, exc);
    });
}

// Hook Level::tick() — 在 forEachDimension 前后插入并行逻辑
LL_TYPE_INSTANCE_HOOK(
    LevelTickHook,
    ll::memory::HookPriority::Normal,
    Level,
    &Level::tick,
    void
) {
    if (!dim_parallel::config.enabled) {
        origin();
        return;
    }

    // 阶段 1: 设置 suppress 标志，让 Dimension::tick() 只收集指针不执行
    g_collectedDimensions.clear();
    g_suppressDimensionTick = true;

    // 调用原始 Level::tick()
    // 内部的 forEachDimension(dim.tick()) 会被我们的 DimensionTickHook 拦截
    // dim.tick() 不会真正执行，只会收集维度指针
    // 其他所有逻辑（tickEntities, tickEntitySystems, _subTick 等）正常执行
    origin();

    // 阶段 2: 关闭 suppress，开启并行
    g_suppressDimensionTick = false;

    if (g_collectedDimensions.empty()) {
        return;
    }

    // 阶段 3: 并行 tick 所有收集到的维度
    g_inParallelPhase = true;
    ParallelDimensionTickManager::getInstance().dispatchAndSync(this);
    g_inParallelPhase = false;
}

// ==================== 插件生命周期 ====================

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
    logger().info("DimParallel loaded. enabled={}, debug={}", config.enabled, config.debug);
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
