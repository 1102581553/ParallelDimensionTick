#include "ParallelDimensionTick.h"

#include <ll/api/io/LoggerRegistry.h>
#include <ll/api/memory/Hook.h>
#include <ll/api/mod/RegisterHelper.h>

#include <mc/network/LoopbackPacketSender.h>
#include <mc/network/Packet.h>
#include <mc/network/PacketSender.h>
#include <mc/server/ServerLevel.h>
#include <mc/world/actor/Actor.h>
#include <mc/world/actor/Mob.h>
#include <mc/world/actor/player/Player.h>
#include <mc/world/level/BlockSource.h>
#include <mc/world/level/Level.h>
#include <mc/world/level/Tick.h>
#include <mc/world/level/dimension/Dimension.h>

#include <Windows.h>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace dim_parallel {

static Config                           config;
static std::shared_ptr<ll::io::Logger> log;
static bool                             hookInstalled = false;

static thread_local DimensionWorkerContext* tl_currentContext   = nullptr;
static thread_local bool                    tl_isWorkerThread   = false;
static thread_local int                     tl_currentDimTypeId = -1;
static thread_local const char*             tl_currentPhase     = "idle";

static std::atomic<bool>       g_inParallelPhase{false};
static std::atomic<bool>       g_suppressDimensionTick{false};
static std::vector<Dimension*> g_collectedDimensions;
static std::mutex              g_collectMutex;

// 危险函数恢复：funcName -> recoverAtTick
static std::unordered_map<std::string, uint64_t> g_dangerousFunctions;
static std::mutex                                g_dangerousMutex;

MainThreadTaskQueue ParallelDimensionTickManager::mMainThreadTasks;

namespace {

template <typename T>
inline void updateMax(std::atomic<T>& target, T value) {
    T expected = target.load(std::memory_order_relaxed);
    while (value > expected) {
        if (target.compare_exchange_weak(expected, value, std::memory_order_relaxed)) {
            break;
        }
    }
}

template <typename T>
inline void updateMaxValue(T& target, T value) {
    if (value > target) {
        target = value;
    }
}

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

template <typename Func>
inline void forwardToMainThread(Func&& func) {
    ParallelDimensionTickManager::runOnMainThread(
        [forwarded = std::forward<Func>(func)]() mutable { forwarded(); }
    );
}

inline bool shouldActorRunOnMainThread(Actor& actor) {
    if (!ParallelDimensionTickManager::isWorkerThread()) {
        return false;
    }

    // Player 全部固定主线程
    if (actor.isPlayer()) {
        return true;
    }

    auto* ctx = ParallelDimensionTickManager::getCurrentContext();
    if (ctx == nullptr || ctx->dimension == nullptr) {
        return false;
    }

    const auto currentDim = ParallelDimensionTickManager::getCurrentDimensionType();
    const auto actorDim   = actor.getDimensionId();

    // actor 已切到别的维度，或者当前 worker 上下文不匹配，直接回主线程
    if (actorDim != currentDim) {
        return true;
    }

    if (static_cast<int>(ctx->dimension->getDimensionId()) != static_cast<int>(actorDim)) {
        return true;
    }

    return false;
}

inline bool shouldPushRunOnMainThread(Actor& self, Actor& other) {
    if (!ParallelDimensionTickManager::isWorkerThread()) {
        return false;
    }

    // 只要有 Player，就统一主线程
    if (self.isPlayer() || other.isPlayer()) {
        return true;
    }

    auto* ctx = ParallelDimensionTickManager::getCurrentContext();
    if (ctx == nullptr || ctx->dimension == nullptr) {
        return false;
    }

    const auto currentDim = ParallelDimensionTickManager::getCurrentDimensionType();
    const auto selfDim    = self.getDimensionId();
    const auto otherDim   = other.getDimensionId();

    // 跨维度、或者任一 actor 已不属于当前 worker 维度，都切主线程
    if (selfDim != currentDim || otherDim != currentDim) {
        return true;
    }

    if (selfDim != otherDim) {
        return true;
    }

    if (static_cast<int>(ctx->dimension->getDimensionId()) != static_cast<int>(selfDim)) {
        return true;
    }

    return false;
}

template <typename Func>
inline void handleDangerousFunction(const char* funcName, Func&& func) {
    if (!config.enabled) {
        std::forward<Func>(func)();
        return;
    }

    if (ParallelDimensionTickManager::isWorkerThread() &&
        ParallelDimensionTickManager::isFunctionDangerous(funcName)) {
        forwardToMainThread(std::forward<Func>(func));
        return;
    }

    std::forward<Func>(func)();
}

template <typename Func>
inline void handleMobFunction(const char* funcName, Actor& actor, Func&& func) {
    if (!config.enabled) {
        std::forward<Func>(func)();
        return;
    }

    if (shouldActorRunOnMainThread(actor)) {
        forwardToMainThread(std::forward<Func>(func));
        return;
    }

    if (ParallelDimensionTickManager::isWorkerThread() &&
        ParallelDimensionTickManager::isFunctionDangerous(funcName)) {
        forwardToMainThread(std::forward<Func>(func));
        return;
    }

    std::forward<Func>(func)();
}

template <typename Func>
inline void handlePlayerFunction(const char* funcName, Func&& func) {
    if (!config.enabled) {
        std::forward<Func>(func)();
        return;
    }

    // Player 永远主线程
    if (ParallelDimensionTickManager::isWorkerThread()) {
        forwardToMainThread(std::forward<Func>(func));
        return;
    }

    (void)funcName;
    std::forward<Func>(func)();
}

template <typename Func>
inline void handlePushFunction(const char* funcName, Actor& self, Actor& other, Func&& func) {
    if (!config.enabled) {
        std::forward<Func>(func)();
        return;
    }

    if (shouldPushRunOnMainThread(self, other)) {
        forwardToMainThread(std::forward<Func>(func));
        return;
    }

    if (ParallelDimensionTickManager::isWorkerThread() &&
        ParallelDimensionTickManager::isFunctionDangerous(funcName)) {
        forwardToMainThread(std::forward<Func>(func));
        return;
    }

    std::forward<Func>(func)();
}

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

MainThreadTaskQueue::ProcessStats MainThreadTaskQueue::processAll() {
    auto start = std::chrono::steady_clock::now();

    {
        std::lock_guard lock(mMutex);
        mProcessing.swap(mTasks);
    }

    ProcessStats stats{};
    stats.total = mProcessing.size();

    for (auto& item : mProcessing) {
        if (item.sync) {
            ++stats.sync;
        } else {
            ++stats.async;
        }

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

    auto end = std::chrono::steady_clock::now();
    stats.elapsedUs = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(end - start).count()
    );
    return stats;
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

uint64_t ParallelDimensionTickManager::getCurrentTick() const {
    return mCurrentTick.load(std::memory_order_acquire);
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
    mCurrentTick.store(0, std::memory_order_release);

    mStats.reset();
    mWindowStats = WindowStats{};

    mInitialized = true;

    logger().info(
        "Initialized per-dimension thread model, recovery interval = {} ticks, debug window = {} ticks",
        RECOVERY_INTERVAL_TICKS,
        DEBUG_WINDOW_TICKS
    );
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

    mCurrentTick.store(0, std::memory_order_release);
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
    auto&    manager   = getInstance();
    uint64_t nowTick   = manager.getCurrentTick();
    uint64_t recoverAt = nowTick + RECOVERY_INTERVAL_TICKS;

    std::lock_guard lock(g_dangerousMutex);

    auto it = g_dangerousFunctions.find(funcName);
    if (it == g_dangerousFunctions.end()) {
        g_dangerousFunctions.emplace(funcName, recoverAt);
        logger().warn(
            "Function '{}' marked as dangerous until tick {} (current tick = {})",
            funcName,
            recoverAt,
            nowTick
        );
    } else {
        if (it->second < recoverAt) {
            it->second = recoverAt;
        }
        logger().warn(
            "Function '{}' danger window extended to tick {} (current tick = {})",
            funcName,
            it->second,
            nowTick
        );
    }

    manager.mStats.totalDangerousFunctions.fetch_add(1, std::memory_order_relaxed);
}

bool ParallelDimensionTickManager::isFunctionDangerous(const std::string& funcName) {
    auto&    manager = getInstance();
    uint64_t nowTick = manager.getCurrentTick();

    std::lock_guard lock(g_dangerousMutex);

    auto it = g_dangerousFunctions.find(funcName);
    if (it == g_dangerousFunctions.end()) {
        return false;
    }

    if (nowTick >= it->second) {
        logger().info("Function '{}' recovered at tick {}", funcName, nowTick);
        g_dangerousFunctions.erase(it);
        return false;
    }

    return true;
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

void ParallelDimensionTickManager::recordDispatchStats(
    uint64_t dispatchUs,
    uint64_t waitUs,
    uint64_t allDimUs,
    size_t   dims
) {
    mStats.totalParallelTicks.fetch_add(1, std::memory_order_relaxed);
    mStats.totalDispatchTimeUs.fetch_add(dispatchUs, std::memory_order_relaxed);
    mStats.totalDispatchWaitTimeUs.fetch_add(waitUs, std::memory_order_relaxed);
    mStats.totalAllDimTickTimeUs.fetch_add(allDimUs, std::memory_order_relaxed);

    updateMax(mStats.maxDispatchTimeUs, dispatchUs);
    updateMax(mStats.maxDispatchWaitTimeUs, waitUs);
    updateMax(mStats.maxAllDimTickTimeUs, allDimUs);

    mWindowStats.parallelTicks++;
    mWindowStats.totalDispatchTimeUs += dispatchUs;
    mWindowStats.totalDispatchWaitTimeUs += waitUs;
    mWindowStats.totalAllDimTickTimeUs += allDimUs;
    mWindowStats.totalDispatchDims += static_cast<uint64_t>(dims);

    updateMaxValue(mWindowStats.maxDispatchTimeUs, dispatchUs);
    updateMaxValue(mWindowStats.maxDispatchWaitTimeUs, waitUs);
    updateMaxValue(mWindowStats.maxAllDimTickTimeUs, allDimUs);
    updateMaxValue(mWindowStats.maxDispatchDims, static_cast<uint64_t>(dims));
}

void ParallelDimensionTickManager::recordFallbackStats(uint64_t fallbackUs) {
    mStats.totalFallbackTicks.fetch_add(1, std::memory_order_relaxed);
    mStats.totalFallbackTimeUs.fetch_add(fallbackUs, std::memory_order_relaxed);
    updateMax(mStats.maxFallbackTimeUs, fallbackUs);

    mWindowStats.fallbackTicks++;
    mWindowStats.totalFallbackTimeUs += fallbackUs;
    updateMaxValue(mWindowStats.maxFallbackTimeUs, fallbackUs);
}

void ParallelDimensionTickManager::maybeLogWindowStats() {
    if (!config.debug) {
        return;
    }

    if (mWindowStats.levelTicks < DEBUG_WINDOW_TICKS) {
        return;
    }

    const WindowStats window = mWindowStats;
    mWindowStats = WindowStats{};

    const uint64_t avgLevelOriginUs = window.levelTicks > 0
        ? (window.totalLevelOriginTimeUs / window.levelTicks)
        : 0;
    const uint64_t avgLevelHookUs = window.levelTicks > 0
        ? (window.totalLevelHookTimeUs / window.levelTicks)
        : 0;

    logger().info(
        "[window {} ticks] levelOrigin avg={}us max={}us  levelHook avg={}us max={}us",
        window.levelTicks,
        avgLevelOriginUs,
        window.maxLevelOriginTimeUs,
        avgLevelHookUs,
        window.maxLevelHookTimeUs
    );

    if (window.parallelTicks > 0) {
        const uint64_t avgDispatchUs = window.totalDispatchTimeUs / window.parallelTicks;
        const uint64_t avgWaitUs     = window.totalDispatchWaitTimeUs / window.parallelTicks;
        const uint64_t avgDimSumUs   = window.totalAllDimTickTimeUs / window.parallelTicks;
        const uint64_t avgTaskProcUs = window.totalMainThreadTaskProcessTimeUs / window.parallelTicks;
        const double   avgDims       = static_cast<double>(window.totalDispatchDims) /
                                     static_cast<double>(window.parallelTicks);
        const double   gain          = window.totalDispatchTimeUs > 0
            ? static_cast<double>(window.totalAllDimTickTimeUs) / static_cast<double>(window.totalDispatchTimeUs)
            : 0.0;

        logger().info(
            "  parallel={} avgDims={:.2f}(max={}) dispatch avg={}us max={}us wait avg={}us max={}us",
            window.parallelTicks,
            avgDims,
            window.maxDispatchDims,
            avgDispatchUs,
            window.maxDispatchTimeUs,
            avgWaitUs,
            window.maxDispatchWaitTimeUs
        );

        logger().info(
            "  dimSum avg={}us max={}us gain={:.2f}x  mainTasks={} (sync={} async={}) taskProcess avg={}us max={}us queue={}",
            avgDimSumUs,
            window.maxAllDimTickTimeUs,
            gain,
            window.totalMainThreadTasks,
            window.totalMainThreadSyncTasks,
            window.totalMainThreadAsyncTasks,
            avgTaskProcUs,
            window.maxMainThreadTaskProcessTimeUs,
            mMainThreadTasks.size()
        );
    } else {
        logger().info("  parallel=0");
    }

    if (window.fallbackTicks > 0) {
        const uint64_t avgFallbackUs = window.totalFallbackTimeUs / window.fallbackTicks;
        logger().info(
            "  fallback={} avg={}us max={}us",
            window.fallbackTicks,
            avgFallbackUs,
            window.maxFallbackTimeUs
        );
    } else {
        logger().info("  fallback=0");
    }

    logger().info(
        "  lifetime: levelTicks={} parallelTicks={} fallbacks={} maxLevelHook={}us maxLevelOrigin={}us maxDispatch={}us maxWait={}us maxFallback={}us maxDimSum={}us maxSingleDim={}us dangerousHits={} recoveryAttempts={}",
        mStats.totalLevelTicks.load(std::memory_order_relaxed),
        mStats.totalParallelTicks.load(std::memory_order_relaxed),
        mStats.totalFallbackTicks.load(std::memory_order_relaxed),
        mStats.maxLevelHookTimeUs.load(std::memory_order_relaxed),
        mStats.maxLevelOriginTimeUs.load(std::memory_order_relaxed),
        mStats.maxDispatchTimeUs.load(std::memory_order_relaxed),
        mStats.maxDispatchWaitTimeUs.load(std::memory_order_relaxed),
        mStats.maxFallbackTimeUs.load(std::memory_order_relaxed),
        mStats.maxAllDimTickTimeUs.load(std::memory_order_relaxed),
        mStats.maxDimTickTimeUs.load(std::memory_order_relaxed),
        mStats.totalDangerousFunctions.load(std::memory_order_relaxed),
        mStats.totalRecoveryAttempts.load(std::memory_order_relaxed)
    );
}

void ParallelDimensionTickManager::recordLevelTickStats(uint64_t levelOriginUs, uint64_t levelHookUs) {
    mStats.totalLevelTicks.fetch_add(1, std::memory_order_relaxed);
    mStats.totalLevelOriginTimeUs.fetch_add(levelOriginUs, std::memory_order_relaxed);
    mStats.totalLevelHookTimeUs.fetch_add(levelHookUs, std::memory_order_relaxed);

    updateMax(mStats.maxLevelOriginTimeUs, levelOriginUs);
    updateMax(mStats.maxLevelHookTimeUs, levelHookUs);

    mWindowStats.levelTicks++;
    mWindowStats.totalLevelOriginTimeUs += levelOriginUs;
    mWindowStats.totalLevelHookTimeUs += levelHookUs;

    updateMaxValue(mWindowStats.maxLevelOriginTimeUs, levelOriginUs);
    updateMaxValue(mWindowStats.maxLevelHookTimeUs, levelHookUs);

    maybeLogWindowStats();
}

void ParallelDimensionTickManager::dispatchAndSync(Level* level, std::vector<Dimension*> dimensions) {
    if (dimensions.empty()) {
        return;
    }

    if (!level || !mInitialized || isStopping()) {
        serialFallbackTick(dimensions);
        return;
    }

    mSnapshot.time      = level->getTime();
    mSnapshot.simPaused = level->getSimPaused();
    mCurrentTick.store(static_cast<uint64_t>(mSnapshot.time), std::memory_order_release);

    if (mSnapshot.simPaused) {
        return;
    }

    auto dispatchStart = std::chrono::steady_clock::now();

    if (dimensions.size() == 1) {
        auto* dim = dimensions[0];
        if (dim != nullptr) {
            auto dimStart = std::chrono::steady_clock::now();
            dim->tick();
            auto dimEnd = std::chrono::steady_clock::now();

            const uint64_t dimUs = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::microseconds>(dimEnd - dimStart).count()
            );
            updateMax(mStats.maxDimTickTimeUs, dimUs);

            const uint64_t dispatchUs = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::microseconds>(dimEnd - dispatchStart).count()
            );
            recordDispatchStats(dispatchUs, 0, dimUs, 1);
        }
        return;
    }

    mActiveDispatches.fetch_add(1, std::memory_order_acq_rel);
    bool dispatchRegistered = true;

    auto leaveDispatch = [this, &dispatchRegistered]() {
        if (!dispatchRegistered) {
            return;
        }
        dispatchRegistered = false;
        mActiveDispatches.fetch_sub(1, std::memory_order_acq_rel);
        notifyDispatchProgress();
    };

    if (isStopping()) {
        leaveDispatch();
        serialFallbackTick(dimensions);
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
            leaveDispatch();
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
                    leaveDispatch();
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
        leaveDispatch();
        return;
    }

    for (auto* ctx : activeContexts) {
        if (ctx == nullptr) {
            mFallbackToSerial.store(true, std::memory_order_release);
            leaveDispatch();
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

    size_t   remaining      = activeContexts.size();
    uint64_t dispatchWaitUs = 0;

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

        auto waitStart = std::chrono::steady_clock::now();
        {
            std::unique_lock dispatchLock(mDispatchMutex);
            mDispatchCV.wait_for(dispatchLock, std::chrono::milliseconds(1));
        }
        auto waitEnd = std::chrono::steady_clock::now();

        dispatchWaitUs += static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(waitEnd - waitStart).count()
        );
    }

    processAllMainThreadTasks();

    uint64_t allDimTickTimeUs = 0;
    for (auto* ctx : activeContexts) {
        if (ctx != nullptr) {
            allDimTickTimeUs += ctx->lastTickTimeUs;
        }
    }

    auto dispatchEnd = std::chrono::steady_clock::now();
    const uint64_t dispatchUs = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(dispatchEnd - dispatchStart).count()
    );

    recordDispatchStats(dispatchUs, dispatchWaitUs, allDimTickTimeUs, activeContexts.size());

    if (!fallbackBeforeDispatch && mFallbackToSerial.load(std::memory_order_acquire)) {
        mFallbackStartTick = currentTick;
    }

    leaveDispatch();
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

    updateMax(mStats.maxDimTickTimeUs, ctx.lastTickTimeUs);

    tl_isWorkerThread   = false;
    tl_currentContext   = nullptr;
    tl_currentDimTypeId = -1;
    tl_currentPhase     = "idle";
}

size_t ParallelDimensionTickManager::processAllMainThreadTasks() {
    auto stats = mMainThreadTasks.processAll();
    if (stats.total > 0) {
        mStats.totalMainThreadTasks.fetch_add(static_cast<uint64_t>(stats.total), std::memory_order_relaxed);
        mStats.totalMainThreadSyncTasks.fetch_add(static_cast<uint64_t>(stats.sync), std::memory_order_relaxed);
        mStats.totalMainThreadAsyncTasks.fetch_add(static_cast<uint64_t>(stats.async), std::memory_order_relaxed);
        mStats.totalMainThreadTaskProcessTimeUs.fetch_add(stats.elapsedUs, std::memory_order_relaxed);
        updateMax(mStats.maxMainThreadTaskProcessTimeUs, stats.elapsedUs);

        mWindowStats.totalMainThreadTasks += static_cast<uint64_t>(stats.total);
        mWindowStats.totalMainThreadSyncTasks += static_cast<uint64_t>(stats.sync);
        mWindowStats.totalMainThreadAsyncTasks += static_cast<uint64_t>(stats.async);
        mWindowStats.totalMainThreadTaskProcessTimeUs += stats.elapsedUs;
        updateMaxValue(mWindowStats.maxMainThreadTaskProcessTimeUs, stats.elapsedUs);
    }
    return stats.total;
}

void ParallelDimensionTickManager::serialFallbackTick(const std::vector<Dimension*>& dimensions) {
    auto start = std::chrono::steady_clock::now();

    for (auto* dim : dimensions) {
        if (dim != nullptr) {
            dim->tick();
        }
    }

    auto end = std::chrono::steady_clock::now();
    const uint64_t fallbackUs = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(end - start).count()
    );

    recordFallbackStats(fallbackUs);
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
    const char* funcName = "Dimension::tickRedstone";
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
    const char* funcName = "Dimension::_sendBlocksChangedPackets";
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
    const char* funcName = "Dimension::_processEntityChunkTransfers";
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
    const char* funcName = "Dimension::_tickEntityChunkMoves";
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
    const char* funcName = "Dimension::_runChunkGenerationWatchdog";
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
    const char* funcName = "Dimension::sendBroadcast";
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
    const char* funcName = "Dimension::sendPacketForPosition";
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
    Actor const&   actor,
    Packet const&  packet,
    Player const*  except
) {
    const char* funcName = "Dimension::sendPacketForEntity";
    ScopedPhase phase(funcName);
    auto* actorPtr  = &actor;
    auto* packetPtr = &packet;

    handleDangerousFunction(funcName, [this, actorPtr, packetPtr, except]() {
        origin(*actorPtr, *packetPtr, except);
    });
}

//------------------------------
// Mob hooks: 纳入维度并行
//------------------------------

LL_TYPE_INSTANCE_HOOK(
    MobBaseTickHook,
    ll::memory::HookPriority::Normal,
    Mob,
    &Mob::$baseTick,
    void
) {
    const char* funcName = "Mob::baseTick";
    ScopedPhase phase(funcName);
    Actor& actor = static_cast<Actor&>(*this);

    handleMobFunction(funcName, actor, [this]() { origin(); });
}

LL_TYPE_INSTANCE_HOOK(
    MobNormalTickHook,
    ll::memory::HookPriority::Normal,
    Mob,
    &Mob::$normalTick,
    void
) {
    const char* funcName = "Mob::normalTick";
    ScopedPhase phase(funcName);
    Actor& actor = static_cast<Actor&>(*this);

    handleMobFunction(funcName, actor, [this]() { origin(); });
}

LL_TYPE_INSTANCE_HOOK(
    MobAiStepHook,
    ll::memory::HookPriority::Normal,
    Mob,
    &Mob::$aiStep,
    void
) {
    const char* funcName = "Mob::aiStep";
    ScopedPhase phase(funcName);
    Actor& actor = static_cast<Actor&>(*this);

    handleMobFunction(funcName, actor, [this]() { origin(); });
}

LL_TYPE_INSTANCE_HOOK(
    MobNewServerAiStepHook,
    ll::memory::HookPriority::Normal,
    Mob,
    &Mob::$newServerAiStep,
    void
) {
    const char* funcName = "Mob::newServerAiStep";
    ScopedPhase phase(funcName);
    Actor& actor = static_cast<Actor&>(*this);

    handleMobFunction(funcName, actor, [this]() { origin(); });
}

//------------------------------
// Player hooks: 永远主线程
//------------------------------

LL_TYPE_INSTANCE_HOOK(
    PlayerNormalTickHook,
    ll::memory::HookPriority::Normal,
    Player,
    &Player::$normalTick,
    void
) {
    const char* funcName = "Player::normalTick";
    ScopedPhase phase(funcName);
    handlePlayerFunction(funcName, [this]() { origin(); });
}

LL_TYPE_INSTANCE_HOOK(
    PlayerAiStepHook,
    ll::memory::HookPriority::Normal,
    Player,
    &Player::$aiStep,
    void
) {
    const char* funcName = "Player::aiStep";
    ScopedPhase phase(funcName);
    handlePlayerFunction(funcName, [this]() { origin(); });
}

LL_TYPE_INSTANCE_HOOK(
    PlayerPassengerTickHook,
    ll::memory::HookPriority::Normal,
    Player,
    &Player::$passengerTick,
    void
) {
    const char* funcName = "Player::passengerTick";
    ScopedPhase phase(funcName);
    handlePlayerFunction(funcName, [this]() { origin(); });
}

LL_TYPE_INSTANCE_HOOK(
    PlayerTickWorldHook,
    ll::memory::HookPriority::Normal,
    Player,
    &Player::$tickWorld,
    void,
    Tick const& currentTick
) {
    const char* funcName = "Player::tickWorld";
    ScopedPhase phase(funcName);
    auto* tickPtr = &currentTick;

    handlePlayerFunction(funcName, [this, tickPtr]() { origin(*tickPtr); });
}

//------------------------------
// Actor push: 危险回退 + recover + 跨维度保护
//------------------------------

LL_TYPE_INSTANCE_HOOK(
    ActorOnPushHook,
    ll::memory::HookPriority::Normal,
    Actor,
    &Actor::$onPush,
    void,
    Actor& other
) {
    const char* funcName = "Actor::onPush";
    ScopedPhase phase(funcName);
    auto* otherPtr = &other;

    handlePushFunction(funcName, *this, *otherPtr, [this, otherPtr]() {
        origin(*otherPtr);
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

    auto hookStart = std::chrono::steady_clock::now();

    {
        std::lock_guard lock(g_collectMutex);
        g_collectedDimensions.clear();
    }

    auto originStart = std::chrono::steady_clock::now();
    {
        ScopedAtomicFlag suppress(g_suppressDimensionTick, true);
        origin();
    }
    auto originEnd = std::chrono::steady_clock::now();

    std::vector<Dimension*> dims;
    {
        std::lock_guard lock(g_collectMutex);
        dims = std::move(g_collectedDimensions);
    }

    if (!dims.empty()) {
        ScopedAtomicFlag parallel(g_inParallelPhase, true);
        ParallelDimensionTickManager::getInstance().dispatchAndSync(this, std::move(dims));
    }

    auto hookEnd = std::chrono::steady_clock::now();

    const uint64_t levelOriginUs = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(originEnd - originStart).count()
    );
    const uint64_t levelHookUs = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(hookEnd - hookStart).count()
    );

    ParallelDimensionTickManager::getInstance().recordLevelTickStats(levelOriginUs, levelHookUs);
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

        MobBaseTickHook::hook();
        MobNormalTickHook::hook();
        MobAiStepHook::hook();
        MobNewServerAiStepHook::hook();

        PlayerNormalTickHook::hook();
        PlayerAiStepHook::hook();
        PlayerPassengerTickHook::hook();
        PlayerTickWorldHook::hook();

        ActorOnPushHook::hook();

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
        ActorOnPushHook::unhook();

        PlayerTickWorldHook::unhook();
        PlayerPassengerTickHook::unhook();
        PlayerAiStepHook::unhook();
        PlayerNormalTickHook::unhook();

        MobNewServerAiStepHook::unhook();
        MobAiStepHook::unhook();
        MobNormalTickHook::unhook();
        MobBaseTickHook::unhook();

        DimensionSendPacketForEntityHook::unhook();
        DimensionSendPacketForPositionHook::unhook();
        DimensionSendBroadcastHook::unhook();
        DimensionRunChunkGenWatchdogHook::unhook();
        DimensionTickEntityChunkMovesHook::unhook();
        DimensionProcessEntityTransfersHook::unhook();
        DimensionSendBlocksChangedHook::unhook();
        DimensionTickRedstoneHook::unhook();

        DimensionTickHook::unhook();
        LevelTickHook::unhook();

        hookInstalled = false;
    }

    logger().info("DimParallel disabled");
    return true;
}

} // namespace dim_parallel

LL_REGISTER_MOD(dim_parallel::PluginImpl, dim_parallel::PluginImpl::getInstance());
