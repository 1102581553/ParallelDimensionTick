#include "ParallelDimensionTick.h"

#include <ll/api/io/LoggerRegistry.h>
#include <ll/api/memory/Hook.h>
#include <ll/api/mod/RegisterHelper.h>

#include <mc/world/actor/Actor.h>
#include <mc/world/actor/Mob.h>
#include <mc/world/actor/player/Player.h>
#include <mc/world/level/Level.h>
#include <mc/world/level/Tick.h>

#include <chrono>
#include <filesystem>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace dim_parallel {

static Config                           config;
static std::shared_ptr<ll::io::Logger> log;
static bool                             hookInstalled = false;

static thread_local DimensionWorkerContext* tl_currentContext      = nullptr;
static thread_local bool                    tl_isWorkerThread      = false;
static thread_local bool                    tl_replayingActorPhase = false;
static thread_local int                     tl_currentDimTypeId    = -1;
static thread_local const char*             tl_currentPhase        = "idle";

// 危险函数恢复：funcName -> recoverAtTick
static std::unordered_map<std::string, uint64_t> g_dangerousFunctions;
static std::mutex                                g_dangerousMutex;

MainThreadTaskQueue ParallelActorTickManager::mMainThreadTasks;

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

class ScopedReplayActorPhase {
public:
    ScopedReplayActorPhase() noexcept : mPrev(tl_replayingActorPhase) { tl_replayingActorPhase = true; }
    ~ScopedReplayActorPhase() noexcept { tl_replayingActorPhase = mPrev; }

private:
    bool mPrev;
};

template <typename Func>
inline void forwardToMainThread(Func&& func) {
    ParallelActorTickManager::runOnMainThread(
        [forwarded = std::forward<Func>(func)]() mutable { forwarded(); }
    );
}

inline bool shouldActorRunOnMainThread(Actor& actor) {
    if (!ParallelActorTickManager::isWorkerThread()) {
        return false;
    }

    if (actor.isPlayer()) {
        return true;
    }

    auto* ctx = ParallelActorTickManager::getCurrentContext();
    if (ctx == nullptr) {
        return false;
    }

    const auto actorDim = actor.getDimensionId();
    return static_cast<int>(actorDim) != ctx->dimensionId;
}

inline bool shouldPushRunOnMainThread(Actor& self, Actor& other) {
    if (!ParallelActorTickManager::isWorkerThread()) {
        return false;
    }

    if (self.isPlayer() || other.isPlayer()) {
        return true;
    }

    auto* ctx = ParallelActorTickManager::getCurrentContext();
    if (ctx == nullptr) {
        return false;
    }

    const auto selfDim  = self.getDimensionId();
    const auto otherDim = other.getDimensionId();

    return static_cast<int>(selfDim) != ctx->dimensionId ||
           static_cast<int>(otherDim) != ctx->dimensionId ||
           selfDim != otherDim;
}

template <typename Func>
inline void handleSensitiveFunction(const char* funcName, Actor& actor, Func&& func) {
    if (!config.enabled) {
        std::forward<Func>(func)();
        return;
    }

    if (shouldActorRunOnMainThread(actor)) {
        forwardToMainThread(std::forward<Func>(func));
        return;
    }

    if (ParallelActorTickManager::isWorkerThread() &&
        ParallelActorTickManager::isFunctionDangerous(funcName)) {
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

    if (ParallelActorTickManager::isWorkerThread()) {
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

    if (ParallelActorTickManager::isWorkerThread() &&
        ParallelActorTickManager::isFunctionDangerous(funcName)) {
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
// ParallelActorTickManager
//=============================================================================

ParallelActorTickManager& ParallelActorTickManager::getInstance() {
    static ParallelActorTickManager instance;
    return instance;
}

int ParallelActorTickManager::phaseOrder(ActorTickPhase phase) {
    switch (phase) {
    case ActorTickPhase::BaseTick: return 1;
    case ActorTickPhase::NormalTick: return 2;
    case ActorTickPhase::AiStep: return 3;
    case ActorTickPhase::PassengerTick: return 4;
    default: return 0;
    }
}

const char* ParallelActorTickManager::phaseName(ActorTickPhase phase) {
    switch (phase) {
    case ActorTickPhase::BaseTick: return "ActorPhase::BaseTick";
    case ActorTickPhase::NormalTick: return "ActorPhase::NormalTick";
    case ActorTickPhase::AiStep: return "ActorPhase::AiStep";
    case ActorTickPhase::PassengerTick: return "ActorPhase::PassengerTick";
    default: return "ActorPhase::None";
    }
}

uint64_t ParallelActorTickManager::getCurrentTick() const {
    return mCurrentTick.load(std::memory_order_acquire);
}

bool ParallelActorTickManager::isStopping() const {
    return mStopping.load(std::memory_order_acquire);
}

void ParallelActorTickManager::initialize() {
    if (mInitialized) {
        return;
    }

    mStopping.store(false, std::memory_order_release);
    mActiveDispatches.store(0, std::memory_order_release);
    mCurrentTick.store(0, std::memory_order_release);

    mStats.reset();
    mWindowStats    = WindowStats{};
    mCurrentPhase   = ActorTickPhase::None;
    mInLevelTick    = false;
    mBypassCurrentTick = false;
    clearCurrentPhaseBuckets();

    mInitialized = true;

    logger().info(
        "Initialized actor parallel model. actorPhase={} recovery={} debugWindow={}",
        config.parallelActorPhase,
        RECOVERY_INTERVAL_TICKS,
        DEBUG_WINDOW_TICKS
    );
}

void ParallelActorTickManager::shutdown() {
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

    clearCurrentPhaseBuckets();
    mCurrentTick.store(0, std::memory_order_release);
    mInitialized = false;
    logger().info("Shutdown");
}

bool ParallelActorTickManager::isWorkerThread() { return tl_isWorkerThread; }

DimensionWorkerContext* ParallelActorTickManager::getCurrentContext() { return tl_currentContext; }

DimensionType ParallelActorTickManager::getCurrentDimensionType() {
    return static_cast<DimensionType>(tl_currentDimTypeId);
}

void ParallelActorTickManager::notifyDispatchProgress() {
    mDispatchCV.notify_one();
}

void ParallelActorTickManager::runOnMainThread(std::function<void()> task) {
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

void ParallelActorTickManager::markFunctionDangerous(const std::string& funcName) {
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
    } else if (it->second < recoverAt) {
        it->second = recoverAt;
        logger().warn(
            "Function '{}' danger window extended to tick {} (current tick = {})",
            funcName,
            it->second,
            nowTick
        );
    }

    manager.mStats.totalDangerousFunctions.fetch_add(1, std::memory_order_relaxed);
}

bool ParallelActorTickManager::isFunctionDangerous(const std::string& funcName) {
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

void ParallelActorTickManager::beginLevelTick(Level* level) {
    if (!mInitialized || !config.parallelActorPhase || !level) {
        mInLevelTick = false;
        return;
    }

    mSnapshot.time      = level->getTime();
    mSnapshot.simPaused = level->getSimPaused();
    mCurrentTick.store(static_cast<uint64_t>(mSnapshot.time), std::memory_order_release);

    mCurrentPhase      = ActorTickPhase::None;
    mInLevelTick       = !mSnapshot.simPaused;
    mBypassCurrentTick = false;
    clearCurrentPhaseBuckets();
}

void ParallelActorTickManager::endLevelTick() {
    if (!mInLevelTick) {
        return;
    }

    flushCurrentPhase();
    mCurrentPhase = ActorTickPhase::None;
    mInLevelTick  = false;
    mBypassCurrentTick = false;
    clearCurrentPhaseBuckets();
}

void ParallelActorTickManager::clearCurrentPhaseBuckets() {
    mCurrentPhaseBuckets.clear();
}

void ParallelActorTickManager::collectActor(Actor& actor, bool isMob) {
    const int dimId = static_cast<int>(actor.getDimensionId());
    auto& bucket = mCurrentPhaseBuckets[dimId];

    auto it = bucket.index.find(&actor);
    if (it == bucket.index.end()) {
        const size_t idx = bucket.items.size();
        bucket.items.push_back(ActorWorkItem{
            .actor = &actor,
            .isMob = isMob,
        });
        bucket.index.emplace(&actor, idx);
    } else {
        bucket.items[it->second].isMob = bucket.items[it->second].isMob || isMob;
    }
}

bool ParallelActorTickManager::interceptActorPhase(Actor& actor, ActorTickPhase phase, bool isMob) {
    if (!mInitialized || !config.parallelActorPhase || !mInLevelTick || mBypassCurrentTick) {
        return false;
    }

    if (actor.isPlayer()) {
        return false;
    }

    const char* curPhaseName = phaseName(phase);
    if (isFunctionDangerous(curPhaseName)) {
        return false;
    }

    if (mCurrentPhase == ActorTickPhase::None) {
        mCurrentPhase = phase;
    } else if (mCurrentPhase != phase) {
        if (phaseOrder(phase) < phaseOrder(mCurrentPhase)) {
            logger().warn(
                "Unexpected actor phase order: current={} next={}, bypassing actor parallel for this tick",
                phaseName(mCurrentPhase),
                phaseName(phase)
            );
            flushCurrentPhase();
            mBypassCurrentTick = true;
            return false;
        }

        flushCurrentPhase();
        mCurrentPhase = phase;
    }

    collectActor(actor, isMob);
    return true;
}

void ParallelActorTickManager::workerLoop(DimensionWorkerContext* ctx) {
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

        processActorPhaseOnWorker(*ctx);

        ctx->jobCompleted.store(true, std::memory_order_release);
        notifyDispatchProgress();
    }

    tl_isWorkerThread   = false;
    tl_currentContext   = nullptr;
    tl_currentDimTypeId = -1;
    tl_currentPhase     = "idle";
}

void ParallelActorTickManager::processActorPhaseOnWorker(DimensionWorkerContext& ctx) {
    tl_isWorkerThread   = true;
    tl_currentContext   = &ctx;
    tl_currentDimTypeId = ctx.dimensionId;
    tl_currentPhase     = phaseName(ctx.phase);

    auto start = std::chrono::steady_clock::now();

    try {
        for (auto& item : ctx.actorTasks) {
            if (item.actor == nullptr) {
                continue;
            }

            if (item.actor->isPlayer()) {
                continue;
            }

            if (static_cast<int>(item.actor->getDimensionId()) != ctx.dimensionId) {
                continue;
            }

            ScopedReplayActorPhase replay;

            switch (ctx.phase) {
            case ActorTickPhase::BaseTick:
                if (item.isMob) {
                    static_cast<Mob*>(item.actor)->baseTick();
                } else {
                    item.actor->baseTick();
                }
                break;
            case ActorTickPhase::NormalTick:
                if (item.isMob) {
                    static_cast<Mob*>(item.actor)->normalTick();
                } else {
                    item.actor->normalTick();
                }
                break;
            case ActorTickPhase::AiStep:
                if (item.isMob) {
                    static_cast<Mob*>(item.actor)->aiStep();
                }
                break;
            case ActorTickPhase::PassengerTick:
                item.actor->passengerTick();
                break;
            default:
                break;
            }
        }
    } catch (const std::exception& e) {
        logger().error(
            "std::exception in actor phase {} dim {}: {}",
            phaseName(ctx.phase),
            ctx.dimensionId,
            e.what()
        );
        markFunctionDangerous(phaseName(ctx.phase));
        mBypassCurrentTick = true;
    } catch (...) {
        logger().error(
            "Unknown exception in actor phase {} dim {}",
            phaseName(ctx.phase),
            ctx.dimensionId
        );
        markFunctionDangerous(phaseName(ctx.phase));
        mBypassCurrentTick = true;
    }

    auto end = std::chrono::steady_clock::now();
    ctx.lastWorkTimeUs = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(end - start).count()
    );
    ctx.lastActorCount = static_cast<uint64_t>(ctx.actorTasks.size());

    updateMax(mStats.maxSingleDimActorTimeUs, ctx.lastWorkTimeUs);
    tl_currentPhase = "idle";
}

void ParallelActorTickManager::flushCurrentPhase() {
    if (mCurrentPhase == ActorTickPhase::None || mCurrentPhaseBuckets.empty()) {
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

    std::vector<DimensionWorkerContext*> activeContexts;
    uint64_t totalActors = 0;

    {
        std::lock_guard contextsLock(mContextsMutex);

        for (auto& [dimId, bucket] : mCurrentPhaseBuckets) {
            if (bucket.items.empty()) {
                continue;
            }

            auto it = mContexts.find(dimId);
            if (it == mContexts.end() || !it->second) {
                auto newCtx = std::make_unique<DimensionWorkerContext>();
                auto* raw   = newCtx.get();
                raw->dimensionId = dimId;
                raw->workerThread = std::thread(&ParallelActorTickManager::workerLoop, this, raw);

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
                    clearCurrentPhaseBuckets();
                    return;
                }
                it = insertIt;
            }

            auto* ctx = it->second.get();
            ctx->dimensionId = dimId;
            ctx->phase       = mCurrentPhase;
            ctx->actorTasks  = std::move(bucket.items);
            ctx->lastWorkTimeUs = 0;
            ctx->lastActorCount = 0;
            ctx->jobCompleted.store(false, std::memory_order_release);

            totalActors += static_cast<uint64_t>(ctx->actorTasks.size());
            activeContexts.push_back(ctx);
        }
    }

    if (activeContexts.empty()) {
        leaveDispatch();
        clearCurrentPhaseBuckets();
        return;
    }

    const auto dispatchStart = std::chrono::steady_clock::now();

    for (auto* ctx : activeContexts) {
        {
            std::lock_guard wakeLock(ctx->wakeMutex);
            ctx->shouldWork = true;
        }
        ctx->wakeCV.notify_one();
    }

    size_t   remaining = activeContexts.size();
    uint64_t waitUs    = 0;

    while (remaining > 0) {
        processAllMainThreadTasks();

        for (auto* ctx : activeContexts) {
            if (ctx != nullptr && ctx->jobCompleted.exchange(false, std::memory_order_acq_rel)) {
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

        waitUs += static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(waitEnd - waitStart).count()
        );
    }

    processAllMainThreadTasks();

    uint64_t workUs = 0;
    for (auto* ctx : activeContexts) {
        workUs += ctx->lastWorkTimeUs;
        ctx->actorTasks.clear();
    }

    const auto dispatchEnd = std::chrono::steady_clock::now();
    const uint64_t dispatchUs = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(dispatchEnd - dispatchStart).count()
    );

    recordActorPhaseStats(dispatchUs, waitUs, workUs, totalActors);
    leaveDispatch();
    clearCurrentPhaseBuckets();
}

void ParallelActorTickManager::recordActorPhaseStats(
    uint64_t dispatchUs,
    uint64_t waitUs,
    uint64_t workUs,
    uint64_t actorCount
) {
    mStats.totalActorDispatches.fetch_add(1, std::memory_order_relaxed);
    mStats.totalActorBatches.fetch_add(actorCount, std::memory_order_relaxed);
    mStats.totalActorDispatchTimeUs.fetch_add(dispatchUs, std::memory_order_relaxed);
    mStats.totalActorWaitTimeUs.fetch_add(waitUs, std::memory_order_relaxed);
    mStats.totalActorWorkTimeUs.fetch_add(workUs, std::memory_order_relaxed);

    updateMax(mStats.maxActorDispatchTimeUs, dispatchUs);
    updateMax(mStats.maxActorWaitTimeUs, waitUs);
    updateMax(mStats.maxActorWorkTimeUs, workUs);

    mWindowStats.actorDispatches++;
    mWindowStats.actorBatches += actorCount;
    mWindowStats.totalActorDispatchTimeUs += dispatchUs;
    mWindowStats.totalActorWaitTimeUs += waitUs;
    mWindowStats.totalActorWorkTimeUs += workUs;

    updateMaxValue(mWindowStats.maxActorDispatchTimeUs, dispatchUs);
    updateMaxValue(mWindowStats.maxActorWaitTimeUs, waitUs);
    updateMaxValue(mWindowStats.maxActorWorkTimeUs, workUs);
}

size_t ParallelActorTickManager::processAllMainThreadTasks() {
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

void ParallelActorTickManager::maybeLogWindowStats() {
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
        "[window {} ticks] levelOrigin avg={}us max={}us levelHook avg={}us max={}us",
        window.levelTicks,
        avgLevelOriginUs,
        window.maxLevelOriginTimeUs,
        avgLevelHookUs,
        window.maxLevelHookTimeUs
    );

    if (window.actorDispatches > 0) {
        const uint64_t avgActorDispatchUs = window.totalActorDispatchTimeUs / window.actorDispatches;
        const uint64_t avgActorWaitUs     = window.totalActorWaitTimeUs / window.actorDispatches;
        const uint64_t avgActorWorkUs     = window.totalActorWorkTimeUs / window.actorDispatches;
        const double   actorGain          = window.totalActorDispatchTimeUs > 0
            ? static_cast<double>(window.totalActorWorkTimeUs) / static_cast<double>(window.totalActorDispatchTimeUs)
            : 0.0;

        logger().info(
            "  actorPhase={} avgActors={:.2f} dispatch avg={}us max={}us wait avg={}us max={}us",
            window.actorDispatches,
            window.actorDispatches > 0
                ? static_cast<double>(window.actorBatches) / static_cast<double>(window.actorDispatches)
                : 0.0,
            avgActorDispatchUs,
            window.maxActorDispatchTimeUs,
            avgActorWaitUs,
            window.maxActorWaitTimeUs
        );

        logger().info(
            "  actorWork avg={}us max={}us gain={:.2f}x",
            avgActorWorkUs,
            window.maxActorWorkTimeUs,
            actorGain
        );
    } else {
        logger().info("  actorPhase=0");
    }

    logger().info(
        "  lifetime: levelTicks={} actorDispatches={} actorBatches={} maxLevelHook={}us maxLevelOrigin={}us maxActorDispatch={}us maxActorWork={}us maxSingleDimActor={}us dangerousHits={} recoveryAttempts={} mainTasks={} queue={}",
        mStats.totalLevelTicks.load(std::memory_order_relaxed),
        mStats.totalActorDispatches.load(std::memory_order_relaxed),
        mStats.totalActorBatches.load(std::memory_order_relaxed),
        mStats.maxLevelHookTimeUs.load(std::memory_order_relaxed),
        mStats.maxLevelOriginTimeUs.load(std::memory_order_relaxed),
        mStats.maxActorDispatchTimeUs.load(std::memory_order_relaxed),
        mStats.maxActorWorkTimeUs.load(std::memory_order_relaxed),
        mStats.maxSingleDimActorTimeUs.load(std::memory_order_relaxed),
        mStats.totalDangerousFunctions.load(std::memory_order_relaxed),
        mStats.totalRecoveryAttempts.load(std::memory_order_relaxed),
        mStats.totalMainThreadTasks.load(std::memory_order_relaxed),
        mMainThreadTasks.size()
    );
}

void ParallelActorTickManager::recordLevelTickStats(uint64_t levelOriginUs, uint64_t levelHookUs) {
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

//=============================================================================
// Hooks
//=============================================================================

LL_TYPE_INSTANCE_HOOK(
    ActorBaseTickHook,
    ll::memory::HookPriority::Normal,
    Actor,
    &Actor::$baseTick,
    void
) {
    if (!config.enabled || !config.parallelActorPhase ||
        ParallelActorTickManager::getInstance().isStopping()) {
        origin();
        return;
    }

    if (tl_replayingActorPhase) {
        origin();
        return;
    }

    if (ParallelActorTickManager::getInstance().interceptActorPhase(
            *this,
            ActorTickPhase::BaseTick,
            false
        )) {
        return;
    }

    const char* funcName = "Actor::baseTick";
    ScopedPhase phase(funcName);
    handleSensitiveFunction(funcName, *this, [this]() { origin(); });
}

LL_TYPE_INSTANCE_HOOK(
    ActorNormalTickHook,
    ll::memory::HookPriority::Normal,
    Actor,
    &Actor::$normalTick,
    void
) {
    if (!config.enabled || !config.parallelActorPhase ||
        ParallelActorTickManager::getInstance().isStopping()) {
        origin();
        return;
    }

    if (tl_replayingActorPhase) {
        origin();
        return;
    }

    if (ParallelActorTickManager::getInstance().interceptActorPhase(
            *this,
            ActorTickPhase::NormalTick,
            false
        )) {
        return;
    }

    const char* funcName = "Actor::normalTick";
    ScopedPhase phase(funcName);
    handleSensitiveFunction(funcName, *this, [this]() { origin(); });
}

LL_TYPE_INSTANCE_HOOK(
    ActorPassengerTickHook,
    ll::memory::HookPriority::Normal,
    Actor,
    &Actor::$passengerTick,
    void
) {
    if (!config.enabled || !config.parallelActorPhase ||
        ParallelActorTickManager::getInstance().isStopping()) {
        origin();
        return;
    }

    if (tl_replayingActorPhase) {
        origin();
        return;
    }

    if (ParallelActorTickManager::getInstance().interceptActorPhase(
            *this,
            ActorTickPhase::PassengerTick,
            false
        )) {
        return;
    }

    const char* funcName = "Actor::passengerTick";
    ScopedPhase phase(funcName);
    handleSensitiveFunction(funcName, *this, [this]() { origin(); });
}

LL_TYPE_INSTANCE_HOOK(
    MobBaseTickHook,
    ll::memory::HookPriority::Normal,
    Mob,
    &Mob::$baseTick,
    void
) {
    if (!config.enabled || !config.parallelActorPhase ||
        ParallelActorTickManager::getInstance().isStopping()) {
        origin();
        return;
    }

    if (tl_replayingActorPhase) {
        origin();
        return;
    }

    Actor& actor = static_cast<Actor&>(*this);

    if (ParallelActorTickManager::getInstance().interceptActorPhase(
            actor,
            ActorTickPhase::BaseTick,
            true
        )) {
        return;
    }

    const char* funcName = "Mob::baseTick";
    ScopedPhase phase(funcName);
    handleSensitiveFunction(funcName, actor, [this]() { origin(); });
}

LL_TYPE_INSTANCE_HOOK(
    MobNormalTickHook,
    ll::memory::HookPriority::Normal,
    Mob,
    &Mob::$normalTick,
    void
) {
    if (!config.enabled || !config.parallelActorPhase ||
        ParallelActorTickManager::getInstance().isStopping()) {
        origin();
        return;
    }

    if (tl_replayingActorPhase) {
        origin();
        return;
    }

    Actor& actor = static_cast<Actor&>(*this);

    if (ParallelActorTickManager::getInstance().interceptActorPhase(
            actor,
            ActorTickPhase::NormalTick,
            true
        )) {
        return;
    }

    const char* funcName = "Mob::normalTick";
    ScopedPhase phase(funcName);
    handleSensitiveFunction(funcName, actor, [this]() { origin(); });
}

LL_TYPE_INSTANCE_HOOK(
    MobAiStepHook,
    ll::memory::HookPriority::Normal,
    Mob,
    &Mob::$aiStep,
    void
) {
    if (!config.enabled || !config.parallelActorPhase ||
        ParallelActorTickManager::getInstance().isStopping()) {
        origin();
        return;
    }

    if (tl_replayingActorPhase) {
        origin();
        return;
    }

    Actor& actor = static_cast<Actor&>(*this);

    if (ParallelActorTickManager::getInstance().interceptActorPhase(
            actor,
            ActorTickPhase::AiStep,
            true
        )) {
        return;
    }

    const char* funcName = "Mob::aiStep";
    ScopedPhase phase(funcName);
    handleSensitiveFunction(funcName, actor, [this]() { origin(); });
}

// Player 固定主线程
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

// 高风险交互在 worker 中回主线程
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

// 顶层 tick：只负责开启和结束 actor 调度窗口
LL_TYPE_INSTANCE_HOOK(
    LevelTickHook,
    ll::memory::HookPriority::Normal,
    Level,
    &Level::$tick,
    void
) {
    if (!config.enabled || ParallelActorTickManager::getInstance().isStopping()) {
        origin();
        return;
    }

    auto hookStart = std::chrono::steady_clock::now();
    auto originStart = std::chrono::steady_clock::now();

    auto& manager = ParallelActorTickManager::getInstance();
    manager.beginLevelTick(this);
    origin();
    manager.endLevelTick();

    auto hookEnd = std::chrono::steady_clock::now();

    const uint64_t levelOriginUs = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(hookEnd - originStart).count()
    );
    const uint64_t levelHookUs = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(hookEnd - hookStart).count()
    );

    manager.recordLevelTickStats(levelOriginUs, levelHookUs);
}

//=============================================================================
// Plugin
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

    logger().info(
        "DimParallel loaded. enabled={} debug={} actorPhase={}",
        config.enabled,
        config.debug,
        config.parallelActorPhase
    );
    return true;
}

bool PluginImpl::enable() {
    if (!hookInstalled) {
        LevelTickHook::hook();

        ActorBaseTickHook::hook();
        ActorNormalTickHook::hook();
        ActorPassengerTickHook::hook();

        MobBaseTickHook::hook();
        MobNormalTickHook::hook();
        MobAiStepHook::hook();

        PlayerNormalTickHook::hook();
        PlayerAiStepHook::hook();
        PlayerPassengerTickHook::hook();
        PlayerTickWorldHook::hook();

        ActorOnPushHook::hook();

        hookInstalled = true;
    }

    ParallelActorTickManager::getInstance().initialize();
    logger().info("DimParallel enabled");
    return true;
}

bool PluginImpl::disable() {
    config.enabled = false;
    ParallelActorTickManager::getInstance().shutdown();

    if (hookInstalled) {
        ActorOnPushHook::unhook();

        PlayerTickWorldHook::unhook();
        PlayerPassengerTickHook::unhook();
        PlayerAiStepHook::unhook();
        PlayerNormalTickHook::unhook();

        MobAiStepHook::unhook();
        MobNormalTickHook::unhook();
        MobBaseTickHook::unhook();

        ActorPassengerTickHook::unhook();
        ActorNormalTickHook::unhook();
        ActorBaseTickHook::unhook();

        LevelTickHook::unhook();

        hookInstalled = false;
    }

    logger().info("DimParallel disabled");
    return true;
}

} // namespace dim_parallel

LL_REGISTER_MOD(dim_parallel::PluginImpl, dim_parallel::PluginImpl::getInstance());
