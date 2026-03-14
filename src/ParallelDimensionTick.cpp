#include "ParallelDimensionTick.h"

#include <ll/api/io/LoggerRegistry.h>
#include <ll/api/memory/Hook.h>
#include <ll/api/mod/RegisterHelper.h>

#include <mc/network/LoopbackPacketSender.h>
#include <mc/network/Packet.h>
#include <mc/network/PacketSender.h>
#include <mc/server/ServerLevel.h>
#include <mc/world/actor/Actor.h>
#include <mc/world/level/BlockSource.h>
#include <mc/world/level/Level.h>
#include <mc/world/level/Tick.h>
#include <mc/world/level/dimension/Dimension.h>

#include <Windows.h>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

namespace dim_parallel {

static Config                          config;
static std::shared_ptr<ll::io::Logger> log;
static bool                            hookInstalled = false;

static thread_local DimensionWorkerContext* tl_currentContext   = nullptr;
static thread_local bool                    tl_isWorkerThread   = false;
static thread_local int                     tl_currentDimTypeId = -1;
static thread_local const char*             tl_currentPhase     = "idle";

static std::atomic<bool>       g_inParallelPhase{false};
static std::atomic<bool>       g_suppressDimensionTick{false};
static std::vector<Dimension*> g_collectedDimensions;
static std::mutex              g_collectMutex;

static std::unordered_set<std::string> g_dangerousFunctions;
static std::mutex                      g_dangerousMutex;

MainThreadTaskQueue ParallelDimensionTickManager::mMainThreadTasks;

namespace {

class ScopedPhase {
public:
    explicit ScopedPhase(const char* phase) noexcept : mPrev(tl_currentPhase) { tl_currentPhase = phase; }
    ~ScopedPhase() noexcept { tl_currentPhase = mPrev; }

private:
    const char* mPrev;
};

class ScopedAtomicFlag {
public:
    explicit ScopedAtomicFlag(std::atomic<bool>& flag, bool value = true) noexcept : mFlag(flag) {
        mFlag.store(value, std::memory_order_release);
    }

    ~ScopedAtomicFlag() noexcept { mFlag.store(false, std::memory_order_release); }

private:
    std::atomic<bool>& mFlag;
};

class ActiveDispatchGuard {
public:
    explicit ActiveDispatchGuard(ParallelDimensionTickManager& manager) noexcept
        : mManager(manager) {
        if (mManager.isStopping()) {
            return;
        }
        mManager.getStats(); // keep object referenced
        mManager.mActiveDispatches.fetch_add(1, std::memory_order_acq_rel);
        if (mManager.isStopping()) {
            mManager.mActiveDispatches.fetch_sub(1, std::memory_order_acq_rel);
            return;
        }
        mActive = true;
    }

    ~ActiveDispatchGuard() noexcept {
        if (!mActive) {
            return;
        }
        mManager.mActiveDispatches.fetch_sub(1, std::memory_order_acq_rel);
        mManager.notifyDispatchProgress();
    }

    bool active() const noexcept { return mActive; }

private:
    ParallelDimensionTickManager& mManager;
    bool                          mActive = false;
};

} // namespace

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
    mTasks.push_back(TaskItem{std::move(task), nullptr});
}

std::shared_ptr<MainThreadTaskQueue::SyncState> MainThreadTaskQueue::enqueueSync(std::function<void()> task) {
    auto sync = std::make_shared<SyncState>();
    {
        std::lock_guard lock(mMutex);
        mTasks.push_back(TaskItem{std::move(task), sync});
    }
    return sync;
}

size_t MainThreadTaskQueue::processAll() {
    {
        std::lock_guard lock(mMutex);
        mProcessing.swap(mTasks);
    }

    const size_t count = mProcessing.size();

    for (auto& item : mProcessing) {
        std::exception_ptr taskException;

        try {
            item.fn();
        } catch (...) {
            taskException = std::current_exception();
        }

        if (item.sync) {
            {
                std::lock_guard syncLock(item.sync->mutex);
                item.sync->done      = true;
                item.sync->exception = taskException;
            }
            item.sync->cv.notify_one();
        } else if (taskException) {
            try {
                std::rethrow_exception(taskException);
            } catch (const std::exception& e) {
                logger().error("Exception in main-thread task: {}", e.what());
            } catch (...) {
                logger().error("Unknown exception in main-thread task");
            }
        }
    }

    mProcessing.clear();
    return count;
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

bool ParallelDimensionTickManager::isStopping() const {
    return mStopping.load(std::memory_order_acquire);
}

void ParallelDimensionTickManager::initialize() {
    if (mInitialized) {
        return;
    }

    mFallbackToSerial.store(false, std::memory_order_release);
    mFallbackStartTick = 0;
    mStopping.store(false, std::memory_order_release);
    mActiveDispatches.store(0, std::memory_order_release);
    mInitialized = true;

    logger().info("Initialized per-dimension thread model, recovery interval = {} ticks", RECOVERY_INTERVAL_TICKS);
}

void ParallelDimensionTickManager::shutdown() {
    if (!mInitialized) {
        return;
    }

    mStopping.store(true, std::memory_order_release);

    while (mActiveDispatches.load(std::memory_order_acquire) > 0) {
        processAllMainThreadTasks();
        std::unique_lock waitLock(mDispatchMutex);
        mDispatchCV.wait_for(waitLock, std::chrono::milliseconds(1));
    }

    processAllMainThreadTasks();

    {
        std::lock_guard contextsLock(mContextsMutex);
        for (auto& [id, ctx] : mContexts) {
            if (ctx && ctx->workerThread.joinable()) {
                {
                    std::lock_guard wakeLock(ctx->wakeMutex);
                    ctx->shutdown   = true;
                    ctx->shouldWork = false;
                }
                ctx->wakeCV.notify_one();
            }
        }
    }

    {
        std::lock_guard contextsLock(mContextsMutex);
        for (auto& [id, ctx] : mContexts) {
            if (ctx && ctx->workerThread.joinable()) {
                ctx->workerThread.join();
            }
        }
        mContexts.clear();
    }

    mInitialized = false;
    logger().info("Shutdown");
}

bool ParallelDimensionTickManager::isWorkerThread() { return tl_isWorkerThread; }

DimensionWorkerContext* ParallelDimensionTickManager::getCurrentContext() { return tl_currentContext; }

DimensionType ParallelDimensionTickManager::getCurrentDimensionType() {
    return static_cast<DimensionType>(tl_currentDimTypeId);
}

void ParallelDimensionTickManager::notifyDispatchProgress() {
    mDispatchCV.notify_one();
}

void ParallelDimensionTickManager::runOnMainThread(std::function<void()> task) {
    if (!tl_isWorkerThread || tl_currentContext == nullptr) {
        task();
        return;
    }

    auto& manager = getInstance();
    auto  sync    = mMainThreadTasks.enqueueSync(std::move(task));

    manager.notifyDispatchProgress();

    std::unique_lock lock(sync->mutex);
    sync->cv.wait(lock, [&sync] { return sync->done; });

    if (sync->exception) {
        std::rethrow_exception(sync->exception);
    }
}

void ParallelDimensionTickManager::markFunctionDangerous(const std::string& funcName) {
    std::lock_guard lock(g_dangerousMutex);
    if (g_dangerousFunctions.insert(funcName).second) {
        logger().warn("Function '{}' marked as dangerous (will run on main thread from now on)", funcName);
        getInstance().mStats.totalDangerousFunctions.fetch_add(1, std::memory_order_relaxed);
    }
}

bool ParallelDimensionTickManager::isFunctionDangerous(const std::string& funcName) {
    std::lock_guard lock(g_dangerousMutex);
    return g_dangerousFunctions.find(funcName) != g_dangerousFunctions.end();
}

void ParallelDimensionTickManager::workerLoop(DimensionWorkerContext* ctx) {
    tl_isWorkerThread   = true;
    tl_currentContext   = ctx;
    tl_currentDimTypeId = -1;
    tl_currentPhase     = "idle";

    while (true) {
        std::unique_lock lock(ctx->wakeMutex);
        ctx->wakeCV.wait(lock, [ctx] { return ctx->shouldWork || ctx->shutdown; });

        if (ctx->shutdown) {
            break;
        }

        ctx->shouldWork = false;
        lock.unlock();

        if (ctx->dimension != nullptr) {
            tickDimensionOnWorker(*ctx);
        }

        ctx->tickCompleted.store(true, std::memory_order_release);
        notifyDispatchProgress();
    }

    tl_isWorkerThread   = false;
    tl_currentContext   = nullptr;
    tl_currentDimTypeId = -1;
    tl_currentPhase     = "idle";
}

void ParallelDimensionTickManager::dispatchAndSync(Level* level, std::vector<Dimension*> dimensions) {
    if (dimensions.empty()) {
        return;
    }

    if (!level || !mInitialized || isStopping()) {
        serialFallbackTick(dimensions);
        return;
    }

    ActiveDispatchGuard dispatchGuard(*this);
    if (!dispatchGuard.active()) {
        serialFallbackTick(dimensions);
        return;
    }

    mSnapshot.time      = level->getTime();
    mSnapshot.simPaused = level->getSimPaused();

    if (mSnapshot.simPaused) {
        return;
    }

    if (dimensions.size() == 1) {
        dimensions[0]->tick();
        return;
    }

    const uint64_t currentTick = static_cast<uint64_t>(mSnapshot.time);
    bool           fallbackBeforeDispatch = mFallbackToSerial.load(std::memory_order_acquire);

    if (fallbackBeforeDispatch) {
        if (currentTick - mFallbackStartTick >= RECOVERY_INTERVAL_TICKS) {
            mStats.totalRecoveryAttempts.fetch_add(1, std::memory_order_relaxed);
            logger().debug("Attempting recovery from fallback mode at tick {}", currentTick);
            mFallbackToSerial.store(false, std::memory_order_release);
            fallbackBeforeDispatch = false;
        } else {
            serialFallbackTick(dimensions);
            return;
        }
    }

    std::vector<DimensionWorkerContext*> activeContexts;
    activeContexts.reserve(dimensions.size());

    {
        std::lock_guard contextsLock(mContextsMutex);

        for (auto* dim : dimensions) {
            if (dim == nullptr) {
                continue;
            }

            const int dimId = static_cast<int>(dim->getDimensionId());
            auto      it    = mContexts.find(dimId);

            if (it == mContexts.end() || !it->second) {
                auto newCtx = std::make_unique<DimensionWorkerContext>();
                auto* raw   = newCtx.get();
                raw->dimension = dim;
                raw->tickCompleted.store(false, std::memory_order_release);
                raw->workerThread = std::thread(&ParallelDimensionTickManager::workerLoop, this, raw);

                auto [insertIt, inserted] = mContexts.emplace(dimId, std::move(newCtx));
                if (!inserted || !insertIt->second) {
                    if (raw->workerThread.joinable()) {
                        {
                            std::lock_guard wakeLock(raw->wakeMutex);
                            raw->shutdown = true;
                        }
                        raw->wakeCV.notify_one();
                        raw->workerThread.join();
                    }
                    serialFallbackTick(dimensions);
                    return;
                }
                it = insertIt;
            }

            it->second->dimension = dim;
            activeContexts.push_back(it->second.get());
        }
    }

    if (activeContexts.empty()) {
        return;
    }

    for (auto* ctx : activeContexts) {
        if (ctx == nullptr) {
            mFallbackToSerial.store(true, std::memory_order_release);
            serialFallbackTick(dimensions);
            return;
        }

        ctx->tickCompleted.store(false, std::memory_order_release);
        {
            std::lock_guard wakeLock(ctx->wakeMutex);
            ctx->shouldWork = true;
        }
        ctx->wakeCV.notify_one();
    }

    size_t remaining = activeContexts.size();

    while (remaining > 0) {
        processAllMainThreadTasks();

        for (auto* ctx : activeContexts) {
            if (ctx != nullptr && ctx->tickCompleted.exchange(false, std::memory_order_acq_rel)) {
                if (remaining > 0) {
                    --remaining;
                }
            }
        }

        if (remaining == 0) {
            break;
        }

        if (isStopping()) {
            break;
        }

        std::unique_lock dispatchLock(mDispatchMutex);
        mDispatchCV.wait_for(dispatchLock, std::chrono::milliseconds(1));
    }

    processAllMainThreadTasks();

    if (!fallbackBeforeDispatch && mFallbackToSerial.load(std::memory_order_acquire)) {
        mFallbackStartTick = currentTick;
    }

    mStats.totalParallelTicks.fetch_add(1, std::memory_order_relaxed);

    if (config.debug && (mStats.totalParallelTicks.load(std::memory_order_relaxed) % 200 == 0)) {
        logger().info(
            "Parallel tick #{}: dims={}  mainTasks={}  fallbacks={}  dangerous={}  recoveryAttempts={}",
            mStats.totalParallelTicks.load(std::memory_order_relaxed),
            activeContexts.size(),
            mStats.totalMainThreadTasks.load(std::memory_order_relaxed),
            mStats.totalFallbackTicks.load(std::memory_order_relaxed),
            mStats.totalDangerousFunctions.load(std::memory_order_relaxed),
            mStats.totalRecoveryAttempts.load(std::memory_order_relaxed)
        );

        std::lock_guard contextsLock(mContextsMutex);
        for (auto& [id, ctx] : mContexts) {
            if (ctx) {
                logger().info("  dim[{}]: {}us", id, ctx->lastTickTimeUs);
            }
        }
    }
}

void ParallelDimensionTickManager::tickDimensionOnWorker(DimensionWorkerContext& ctx) {
    tl_isWorkerThread = true;
    tl_currentContext = &ctx;
    tl_currentDimTypeId = (ctx.dimension != nullptr)
        ? static_cast<int>(ctx.dimension->getDimensionId())
        : -1;
    tl_currentPhase = "pre-tick";

    auto start = std::chrono::steady_clock::now();
    bool exceptionOccurred = false;

    try {
        tl_currentPhase = "tick";
        ctx.dimension->tick();
        tl_currentPhase = "post-tick";
    } catch (const std::exception& e) {
        exceptionOccurred = true;
        logger().error("std::exception in dim {} during [{}]: {}", tl_currentDimTypeId, tl_currentPhase, e.what());

        if (tl_currentPhase != nullptr && tl_currentPhase[0] != '\0' &&
            std::strcmp(tl_currentPhase, "pre-tick") != 0 &&
            std::strcmp(tl_currentPhase, "post-tick") != 0 &&
            std::strcmp(tl_currentPhase, "tick") != 0) {
            markFunctionDangerous(tl_currentPhase);
        }
    } catch (...) {
        exceptionOccurred = true;
        logger().error("Unknown exception in dim {} during [{}]", tl_currentDimTypeId, tl_currentPhase);

        if (tl_currentPhase != nullptr && tl_currentPhase[0] != '\0' &&
            std::strcmp(tl_currentPhase, "pre-tick") != 0 &&
            std::strcmp(tl_currentPhase, "post-tick") != 0 &&
            std::strcmp(tl_currentPhase, "tick") != 0) {
            markFunctionDangerous(tl_currentPhase);
        }
    }

    if (exceptionOccurred) {
        mFallbackToSerial.store(true, std::memory_order_release);
    }

    const auto end = std::chrono::steady_clock::now();
    ctx.lastTickTimeUs = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(end - start).count()
    );

    uint64_t expected = mStats.maxDimTickTimeUs.load(std::memory_order_relaxed);
    while (ctx.lastTickTimeUs > expected) {
        if (mStats.maxDimTickTimeUs.compare_exchange_weak(expected, ctx.lastTickTimeUs, std::memory_order_relaxed)) {
            break;
        }
    }

    tl_isWorkerThread   = false;
    tl_currentContext   = nullptr;
    tl_currentDimTypeId = -1;
    tl_currentPhase     = "idle";
}

size_t ParallelDimensionTickManager::processAllMainThreadTasks() {
    const size_t count = mMainThreadTasks.processAll();
    if (count > 0) {
        mStats.totalMainThreadTasks.fetch_add(static_cast<uint64_t>(count), std::memory_order_relaxed);
    }
    return count;
}

void ParallelDimensionTickManager::serialFallbackTick(const std::vector<Dimension*>& dimensions) {
    for (auto* dim : dimensions) {
        if (dim != nullptr) {
            dim->tick();
        }
    }
    mStats.totalFallbackTicks.fetch_add(1, std::memory_order_relaxed);
}

//=============================================================================
// Helper template for dangerous function forwarding
//=============================================================================

template <typename Func>
inline void handleDangerousFunction(const char* funcName, Func&& func) {
    if (!config.enabled) {
        std::forward<Func>(func)();
        return;
    }

    if (ParallelDimensionTickManager::isWorkerThread() &&
        ParallelDimensionTickManager::isFunctionDangerous(funcName)) {
        ParallelDimensionTickManager::runOnMainThread(
            [forwarded = std::forward<Func>(func)]() mutable { forwarded(); }
        );
        return;
    }

    std::forward<Func>(func)();
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
    ScopedPhase phase(funcName);
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
    ScopedPhase phase(funcName);
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
    ScopedPhase phase(funcName);
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
    ScopedPhase phase(funcName);
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
    ScopedPhase phase(funcName);
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
    ScopedPhase phase(funcName);
    auto* packetPtr = &packet;

    handleDangerousFunction(funcName, [this, packetPtr, except]() {
        origin(*packetPtr, except);
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
    const char* funcName = "sendPacketForPosition";
    ScopedPhase phase(funcName);
    auto* positionPtr = &position;
    auto* packetPtr   = &packet;

    handleDangerousFunction(funcName, [this, positionPtr, packetPtr, except]() {
        origin(*positionPtr, *packetPtr, except);
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
    const char* funcName = "sendPacketForEntity";
    ScopedPhase phase(funcName);
    auto* actorPtr  = &actor;
    auto* packetPtr = &packet;

    handleDangerousFunction(funcName, [this, actorPtr, packetPtr, except]() {
        origin(*actorPtr, *packetPtr, except);
    });
}

LL_TYPE_INSTANCE_HOOK(
    DimensionTickHook,
    ll::memory::HookPriority::Normal,
    Dimension,
    &Dimension::$tick,
    void
) {
    if (!config.enabled || ParallelDimensionTickManager::getInstance().isStopping()) {
        origin();
        return;
    }

    if (g_suppressDimensionTick.load(std::memory_order_acquire)) {
        std::lock_guard lock(g_collectMutex);
        g_collectedDimensions.push_back(this);
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
    if (!config.enabled || ParallelDimensionTickManager::getInstance().isStopping()) {
        origin();
        return;
    }

    {
        std::lock_guard lock(g_collectMutex);
        g_collectedDimensions.clear();
    }

    {
        ScopedAtomicFlag suppress(g_suppressDimensionTick, true);
        origin();
    }

    std::vector<Dimension*> dims;
    {
        std::lock_guard lock(g_collectMutex);
        dims = std::move(g_collectedDimensions);
    }

    if (!dims.empty()) {
        ScopedAtomicFlag parallel(g_inParallelPhase, true);
        ParallelDimensionTickManager::getInstance().dispatchAndSync(this, std::move(dims));
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
    config.enabled = false;
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
