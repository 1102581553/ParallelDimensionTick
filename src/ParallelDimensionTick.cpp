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

#include <bit>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <optional>
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
static thread_local bool                    tl_replayingActorPhase = false;
static thread_local int                     tl_currentDimTypeId = -1;
static thread_local const char*             tl_currentPhase     = "idle";

static std::atomic<bool>       g_suppressDimensionTick{false};
static std::atomic<bool>       g_suppressActorTicks{false};
static std::vector<Dimension*> g_collectedDimensions;
static std::mutex              g_collectDimensionMutex;

struct CollectedActorBucket {
    std::vector<ActorWorkItem>            items;
    std::unordered_map<Actor*, size_t>    index;
};

static std::unordered_map<int, CollectedActorBucket> g_collectedActors;
static std::mutex                                     g_collectActorMutex;

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

inline uint64_t popcountMask(ActorPhaseMask mask) {
    return static_cast<uint64_t>(std::popcount(static_cast<uint32_t>(mask)));
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

class ScopedReplayActorPhase {
public:
    ScopedReplayActorPhase() noexcept : mPrev(tl_replayingActorPhase) { tl_replayingActorPhase = true; }
    ~ScopedReplayActorPhase() noexcept { tl_replayingActorPhase = mPrev; }

private:
    bool mPrev;
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

    if (actor.isPlayer()) {
        return true;
    }

    auto* ctx = ParallelDimensionTickManager::getCurrentContext();
    if (ctx == nullptr || ctx->dimension == nullptr) {
        return false;
    }

    const auto currentDim = ParallelDimensionTickManager::getCurrentDimensionType();
    const auto actorDim   = actor.getDimensionId();

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

inline void collectActorPhase(Actor& actor, ActorPhaseMask phase, bool isMob) {
    const int dimId = static_cast<int>(actor.getDimensionId());

    std::lock_guard lock(g_collectActorMutex);
    auto& bucket = g_collectedActors[dimId];

    auto it = bucket.index.find(&actor);
    if (it == bucket.index.end()) {
        const size_t idx = bucket.items.size();
        bucket.items.push_back(ActorWorkItem{
            .actor     = &actor,
            .phaseMask = phase,
            .isMob     = isMob,
        });
        bucket.index.emplace(&actor, idx);
    } else {
        auto& item = bucket.items[it->second];
        item.phaseMask |= phase;
        item.isMob = item.isMob || isMob;
    }
}

inline std::unordered_map<int, std::vector<ActorWorkItem>> takeCollectedActors() {
    std::unordered_map<int, std::vector<ActorWorkItem>> result;
    std::lock_guard lock(g_collectActorMutex);

    result.reserve(g_collectedActors.size());
    for (auto& [dimId, bucket] : g_collectedActors) {
        result.emplace(dimId, std::move(bucket.items));
    }
    g_collectedActors.clear();
    return result;
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
inline void handleActorSensitiveFunction(const char* funcName, Actor& actor, Func&& func) {
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

inline void clearCollections() {
    {
        std::lock_guard lock(g_collectDimensionMutex);
        g_collectedDimensions.clear();
    }
    {
        std::lock_guard lock(g_collectActorMutex);
        g_collectedActors.clear();
    }
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
    clearCollections();

    mInitialized = true;

    logger().info(
        "Initialized dual-phase parallel model. actorPhase={} dimensionPhase={} recovery={} debugWindow={}",
        config.parallelActorPhase,
        config.parallelDimensionPhase,
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

    clearCollections();
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
        const auto jobType = ctx->jobType;
        lock.unlock();

        switch (jobType) {
        case DimensionWorkerContext::JobType::ActorPhase:
            processActorPhaseOnWorker(*ctx);
            break;
        case DimensionWorkerContext::JobType::DimensionPhase:
            if (ctx->dimension != nullptr) {
                tickDimensionOnWorker(*ctx);
            }
            break;
        default:
            break;
        }

        ctx->jobCompleted.store(true, std::memory_order_release);
        notifyDispatchProgress();
    }

    tl_isWorkerThread   = false;
    tl_currentContext   = nullptr;
    tl_currentDimTypeId = -1;
    tl_currentPhase     = "idle";
}

void ParallelDimensionTickManager::processActorPhaseOnWorker(DimensionWorkerContext& ctx) {
    tl_isWorkerThread   = true;
    tl_currentContext   = &ctx;
    tl_currentDimTypeId = ctx.dimensionId;
    tl_currentPhase     = "actor-phase";

    auto start = std::chrono::steady_clock::now();

    uint64_t phaseCalls = 0;

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

        try {
            ScopedReplayActorPhase replay;

            if ((item.phaseMask & ActorPhaseMask::BaseTick) != ActorPhaseMask::None) {
                ScopedPhase phase("Actor::baseTick");
                item.actor->baseTick();
                ++phaseCalls;
            }

            if ((item.phaseMask & ActorPhaseMask::NormalTick) != ActorPhaseMask::None) {
                ScopedPhase phase("Actor::normalTick");
                item.actor->normalTick();
                ++phaseCalls;
            }

            if (item.isMob && (item.phaseMask & ActorPhaseMask::MobAiStep) != ActorPhaseMask::None) {
                if (auto* mob = static_cast<Mob*>(item.actor); mob != nullptr) {
                    ScopedPhase phase("Mob::aiStep");
                    mob->aiStep();
                    ++phaseCalls;
                }
            }

            if ((item.phaseMask & ActorPhaseMask::PassengerTick) != ActorPhaseMask::None) {
                ScopedPhase phase("Actor::passengerTick");
                item.actor->passengerTick();
                ++phaseCalls;
            }
        } catch (const std::exception& e) {
            logger().error(
                "std::exception in actor-phase dim {} during [{}]: {}",
                ctx.dimensionId,
                tl_currentPhase,
                e.what()
            );
            if (tl_currentPhase != nullptr && tl_currentPhase[0] != '\0') {
                markFunctionDangerous(tl_currentPhase);
            }
        } catch (...) {
            logger().error(
                "Unknown exception in actor-phase dim {} during [{}]",
                ctx.dimensionId,
                tl_currentPhase
            );
            if (tl_currentPhase != nullptr && tl_currentPhase[0] != '\0') {
                markFunctionDangerous(tl_currentPhase);
            }
        }
    }

    auto end = std::chrono::steady_clock::now();

    ctx.lastActorPhaseTimeUs = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(end - start).count()
    );
    ctx.lastActorCount = static_cast<uint64_t>(ctx.actorTasks.size());
    ctx.lastActorPhaseCallCount = phaseCalls;

    tl_currentPhase = "idle";
}

void ParallelDimensionTickManager::tickDimensionOnWorker(DimensionWorkerContext& ctx) {
    tl_isWorkerThread   = true;
    tl_currentContext   = &ctx;
    tl_currentDimTypeId = (ctx.dimension != nullptr)
        ? static_cast<int>(ctx.dimension->getDimensionId())
        : ctx.dimensionId;
    tl_currentPhase = "pre-dimension-tick";

    auto start = std::chrono::steady_clock::now();
    bool exceptionOccurred = false;

    try {
        tl_currentPhase = "Dimension::tick";
        ctx.dimension->tick();
        tl_currentPhase = "post-dimension-tick";
    } catch (const std::exception& e) {
        exceptionOccurred = true;
        logger().error(
            "std::exception in dim {} during [{}]: {}",
            tl_currentDimTypeId,
            tl_currentPhase,
            e.what()
        );

        if (tl_currentPhase != nullptr && tl_currentPhase[0] != '\0' &&
            std::strcmp(tl_currentPhase, "pre-dimension-tick") != 0 &&
            std::strcmp(tl_currentPhase, "post-dimension-tick") != 0) {
            markFunctionDangerous(tl_currentPhase);
        }
    } catch (...) {
        exceptionOccurred = true;
        logger().error("Unknown exception in dim {} during [{}]", tl_currentDimTypeId, tl_currentPhase);

        if (tl_currentPhase != nullptr && tl_currentPhase[0] != '\0' &&
            std::strcmp(tl_currentPhase, "pre-dimension-tick") != 0 &&
            std::strcmp(tl_currentPhase, "post-dimension-tick") != 0) {
            markFunctionDangerous(tl_currentPhase);
        }
    }

    if (exceptionOccurred) {
        mFallbackToSerial.store(true, std::memory_order_release);
    }

    auto end = std::chrono::steady_clock::now();
    ctx.lastDimensionTickTimeUs = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(end - start).count()
    );

    updateMax(mStats.maxSingleDimTickTimeUs, ctx.lastDimensionTickTimeUs);

    tl_currentPhase = "idle";
}

void ParallelDimensionTickManager::recordActorPhaseStats(
    uint64_t dispatchUs,
    uint64_t waitUs,
    uint64_t workUs,
    uint64_t actorCount,
    uint64_t phaseCalls
) {
    mStats.totalActorDispatches.fetch_add(1, std::memory_order_relaxed);
    mStats.totalActorBatches.fetch_add(actorCount, std::memory_order_relaxed);
    mStats.totalActorPhaseCalls.fetch_add(phaseCalls, std::memory_order_relaxed);
    mStats.totalActorDispatchTimeUs.fetch_add(dispatchUs, std::memory_order_relaxed);
    mStats.totalActorWaitTimeUs.fetch_add(waitUs, std::memory_order_relaxed);
    mStats.totalActorWorkTimeUs.fetch_add(workUs, std::memory_order_relaxed);

    updateMax(mStats.maxActorDispatchTimeUs, dispatchUs);
    updateMax(mStats.maxActorWaitTimeUs, waitUs);
    updateMax(mStats.maxActorWorkTimeUs, workUs);

    mWindowStats.actorDispatches++;
    mWindowStats.actorBatches += actorCount;
    mWindowStats.actorPhaseCalls += phaseCalls;
    mWindowStats.totalActorDispatchTimeUs += dispatchUs;
    mWindowStats.totalActorWaitTimeUs += waitUs;
    mWindowStats.totalActorWorkTimeUs += workUs;

    updateMaxValue(mWindowStats.maxActorDispatchTimeUs, dispatchUs);
    updateMaxValue(mWindowStats.maxActorWaitTimeUs, waitUs);
    updateMaxValue(mWindowStats.maxActorWorkTimeUs, workUs);
}

void ParallelDimensionTickManager::recordDimensionPhaseStats(
    uint64_t dispatchUs,
    uint64_t waitUs,
    uint64_t allDimUs,
    size_t   dims
) {
    mStats.totalParallelDimensionTicks.fetch_add(1, std::memory_order_relaxed);
    mStats.totalDimensionDispatchTimeUs.fetch_add(dispatchUs, std::memory_order_relaxed);
    mStats.totalDimensionWaitTimeUs.fetch_add(waitUs, std::memory_order_relaxed);
    mStats.totalAllDimTickTimeUs.fetch_add(allDimUs, std::memory_order_relaxed);

    updateMax(mStats.maxDimensionDispatchTimeUs, dispatchUs);
    updateMax(mStats.maxDimensionWaitTimeUs, waitUs);
    updateMax(mStats.maxAllDimTickTimeUs, allDimUs);

    mWindowStats.parallelDimensionTicks++;
    mWindowStats.totalDimensionDispatchTimeUs += dispatchUs;
    mWindowStats.totalDimensionWaitTimeUs += waitUs;
    mWindowStats.totalAllDimTickTimeUs += allDimUs;
    mWindowStats.totalDispatchDims += static_cast<uint64_t>(dims);

    updateMaxValue(mWindowStats.maxDimensionDispatchTimeUs, dispatchUs);
    updateMaxValue(mWindowStats.maxDimensionWaitTimeUs, waitUs);
    updateMaxValue(mWindowStats.maxAllDimTickTimeUs, allDimUs);
    updateMaxValue(mWindowStats.maxDispatchDims, static_cast<uint64_t>(dims));
}

void ParallelDimensionTickManager::recordFallbackStats(uint64_t fallbackUs) {
    mStats.totalFallbackDimensionTicks.fetch_add(1, std::memory_order_relaxed);
    mStats.totalFallbackTimeUs.fetch_add(fallbackUs, std::memory_order_relaxed);
    updateMax(mStats.maxFallbackTimeUs, fallbackUs);

    mWindowStats.fallbackDimensionTicks++;
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

    if (window.actorDispatches > 0) {
        const uint64_t avgActorDispatchUs = window.totalActorDispatchTimeUs / window.actorDispatches;
        const uint64_t avgActorWaitUs     = window.totalActorWaitTimeUs / window.actorDispatches;
        const uint64_t avgActorWorkUs     = window.totalActorWorkTimeUs / window.actorDispatches;
        const double   actorGain          = window.totalActorDispatchTimeUs > 0
            ? static_cast<double>(window.totalActorWorkTimeUs) / static_cast<double>(window.totalActorDispatchTimeUs)
            : 0.0;

        logger().info(
            "  actorPhase={} avgActors={:.2f} avgPhaseCalls={:.2f} dispatch avg={}us max={}us wait avg={}us max={}us",
            window.actorDispatches,
            window.actorDispatches > 0
                ? static_cast<double>(window.actorBatches) / static_cast<double>(window.actorDispatches)
                : 0.0,
            window.actorDispatches > 0
                ? static_cast<double>(window.actorPhaseCalls) / static_cast<double>(window.actorDispatches)
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

    if (window.parallelDimensionTicks > 0) {
        const uint64_t avgDispatchUs = window.totalDimensionDispatchTimeUs / window.parallelDimensionTicks;
        const uint64_t avgWaitUs     = window.totalDimensionWaitTimeUs / window.parallelDimensionTicks;
        const uint64_t avgDimSumUs   = window.totalAllDimTickTimeUs / window.parallelDimensionTicks;
        const double   avgDims       = static_cast<double>(window.totalDispatchDims) /
                                     static_cast<double>(window.parallelDimensionTicks);
        const double   gain          = window.totalDimensionDispatchTimeUs > 0
            ? static_cast<double>(window.totalAllDimTickTimeUs) / static_cast<double>(window.totalDimensionDispatchTimeUs)
            : 0.0;

        logger().info(
            "  dimensionPhase={} avgDims={:.2f}(max={}) dispatch avg={}us max={}us wait avg={}us max={}us",
            window.parallelDimensionTicks,
            avgDims,
            window.maxDispatchDims,
            avgDispatchUs,
            window.maxDimensionDispatchTimeUs,
            avgWaitUs,
            window.maxDimensionWaitTimeUs
        );

        logger().info(
            "  dimSum avg={}us max={}us gain={:.2f}x",
            avgDimSumUs,
            window.maxAllDimTickTimeUs,
            gain
        );
    } else {
        logger().info("  dimensionPhase=0");
    }

    if (window.fallbackDimensionTicks > 0) {
        const uint64_t avgFallbackUs = window.totalFallbackTimeUs / window.fallbackDimensionTicks;
        logger().info(
            "  fallback={} avg={}us max={}us",
            window.fallbackDimensionTicks,
            avgFallbackUs,
            window.maxFallbackTimeUs
        );
    } else {
        logger().info("  fallback=0");
    }

    logger().info(
        "  lifetime: levelTicks={} actorDispatches={} actorBatches={} actorPhaseCalls={} dimDispatches={} dimFallbacks={} maxLevelHook={}us maxLevelOrigin={}us maxActorDispatch={}us maxDimDispatch={}us maxDimSum={}us maxSingleDim={}us dangerousHits={} recoveryAttempts={} mainTasks={} queue={}",
        mStats.totalLevelTicks.load(std::memory_order_relaxed),
        mStats.totalActorDispatches.load(std::memory_order_relaxed),
        mStats.totalActorBatches.load(std::memory_order_relaxed),
        mStats.totalActorPhaseCalls.load(std::memory_order_relaxed),
        mStats.totalParallelDimensionTicks.load(std::memory_order_relaxed),
        mStats.totalFallbackDimensionTicks.load(std::memory_order_relaxed),
        mStats.maxLevelHookTimeUs.load(std::memory_order_relaxed),
        mStats.maxLevelOriginTimeUs.load(std::memory_order_relaxed),
        mStats.maxActorDispatchTimeUs.load(std::memory_order_relaxed),
        mStats.maxDimensionDispatchTimeUs.load(std::memory_order_relaxed),
        mStats.maxAllDimTickTimeUs.load(std::memory_order_relaxed),
        mStats.maxSingleDimTickTimeUs.load(std::memory_order_relaxed),
        mStats.totalDangerousFunctions.load(std::memory_order_relaxed),
        mStats.totalRecoveryAttempts.load(std::memory_order_relaxed),
        mStats.totalMainThreadTasks.load(std::memory_order_relaxed),
        mMainThreadTasks.size()
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

void ParallelDimensionTickManager::dispatchActorPhase(
    Level* level,
    std::unordered_map<int, std::vector<ActorWorkItem>> tasksByDim
) {
    if (tasksByDim.empty()) {
        return;
    }

    if (!level || !mInitialized || isStopping()) {
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
    uint64_t totalPhases = 0;

    {
        std::lock_guard contextsLock(mContextsMutex);

        for (auto& [dimId, tasks] : tasksByDim) {
            if (tasks.empty()) {
                continue;
            }

            auto it = mContexts.find(dimId);
            if (it == mContexts.end() || !it->second) {
                auto newCtx = std::make_unique<DimensionWorkerContext>();
                auto* raw   = newCtx.get();
                raw->dimensionId = dimId;
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
                    return;
                }
                it = insertIt;
            }

            auto* ctx = it->second.get();
            ctx->dimensionId = dimId;
            ctx->actorTasks = std::move(tasks);
            ctx->lastActorPhaseTimeUs = 0;
            ctx->lastActorCount = 0;
            ctx->lastActorPhaseCallCount = 0;
            ctx->jobType = DimensionWorkerContext::JobType::ActorPhase;
            ctx->jobCompleted.store(false, std::memory_order_release);

            totalActors += static_cast<uint64_t>(ctx->actorTasks.size());
            for (auto const& item : ctx->actorTasks) {
                totalPhases += popcountMask(item.phaseMask);
            }

            activeContexts.push_back(ctx);
        }
    }

    if (activeContexts.empty()) {
        leaveDispatch();
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
        workUs += ctx->lastActorPhaseTimeUs;
        ctx->actorTasks.clear();
        ctx->jobType = DimensionWorkerContext::JobType::None;
    }

    const auto dispatchEnd = std::chrono::steady_clock::now();
    const uint64_t dispatchUs = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(dispatchEnd - dispatchStart).count()
    );

    recordActorPhaseStats(dispatchUs, waitUs, workUs, totalActors, totalPhases);
    leaveDispatch();
}

void ParallelDimensionTickManager::dispatchDimensionPhase(Level* level, std::vector<Dimension*> dimensions) {
    if (dimensions.empty()) {
        return;
    }

    if (!level || !mInitialized || isStopping()) {
        serialFallbackDimensionTick(dimensions);
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
            updateMax(mStats.maxSingleDimTickTimeUs, dimUs);

            const uint64_t dispatchUs = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::microseconds>(dimEnd - dispatchStart).count()
            );
            recordDimensionPhaseStats(dispatchUs, 0, dimUs, 1);
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
        serialFallbackDimensionTick(dimensions);
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
            serialFallbackDimensionTick(dimensions);
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
                raw->dimensionId = dimId;
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
                    serialFallbackDimensionTick(dimensions);
                    return;
                }
                it = insertIt;
            }

            it->second->dimension = dim;
            it->second->dimensionId = dimId;
            it->second->lastDimensionTickTimeUs = 0;
            it->second->jobType = DimensionWorkerContext::JobType::DimensionPhase;
            it->second->jobCompleted.store(false, std::memory_order_release);
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
            serialFallbackDimensionTick(dimensions);
            return;
        }

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

        dispatchWaitUs += static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(waitEnd - waitStart).count()
        );
    }

    processAllMainThreadTasks();

    uint64_t allDimTickTimeUs = 0;
    for (auto* ctx : activeContexts) {
        if (ctx != nullptr) {
            allDimTickTimeUs += ctx->lastDimensionTickTimeUs;
            ctx->jobType = DimensionWorkerContext::JobType::None;
        }
    }

    auto dispatchEnd = std::chrono::steady_clock::now();
    const uint64_t dispatchUs = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(dispatchEnd - dispatchStart).count()
    );

    recordDimensionPhaseStats(dispatchUs, dispatchWaitUs, allDimTickTimeUs, activeContexts.size());

    if (!fallbackBeforeDispatch && mFallbackToSerial.load(std::memory_order_acquire)) {
        mFallbackStartTick = currentTick;
    }

    leaveDispatch();
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

void ParallelDimensionTickManager::serialFallbackDimensionTick(const std::vector<Dimension*>& dimensions) {
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
// Hooks: dimension side
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

LL_TYPE_INSTANCE_HOOK(
    DimensionTickHook,
    ll::memory::HookPriority::Normal,
    Dimension,
    &Dimension::$tick,
    void
) {
    if (!config.enabled || !config.parallelDimensionPhase ||
        ParallelDimensionTickManager::getInstance().isStopping()) {
        origin();
        return;
    }

    if (g_suppressDimensionTick.load(std::memory_order_acquire)) {
        std::lock_guard lock(g_collectDimensionMutex);
        g_collectedDimensions.push_back(this);
        return;
    }

    origin();
}

//=============================================================================
// Hooks: actor collection / replay
//=============================================================================

LL_TYPE_INSTANCE_HOOK(
    ActorBaseTickHook,
    ll::memory::HookPriority::Normal,
    Actor,
    &Actor::$baseTick,
    void
) {
    if (!config.enabled || !config.parallelActorPhase ||
        ParallelDimensionTickManager::getInstance().isStopping()) {
        origin();
        return;
    }

    if (tl_replayingActorPhase) {
        origin();
        return;
    }

    if (g_suppressActorTicks.load(std::memory_order_acquire) && !this->isPlayer()) {
        collectActorPhase(*this, ActorPhaseMask::BaseTick, false);
        return;
    }

    const char* funcName = "Actor::baseTick";
    ScopedPhase phase(funcName);
    handleActorSensitiveFunction(funcName, *this, [this]() { origin(); });
}

LL_TYPE_INSTANCE_HOOK(
    ActorNormalTickHook,
    ll::memory::HookPriority::Normal,
    Actor,
    &Actor::$normalTick,
    void
) {
    if (!config.enabled || !config.parallelActorPhase ||
        ParallelDimensionTickManager::getInstance().isStopping()) {
        origin();
        return;
    }

    if (tl_replayingActorPhase) {
        origin();
        return;
    }

    if (g_suppressActorTicks.load(std::memory_order_acquire) && !this->isPlayer()) {
        collectActorPhase(*this, ActorPhaseMask::NormalTick, false);
        return;
    }

    const char* funcName = "Actor::normalTick";
    ScopedPhase phase(funcName);
    handleActorSensitiveFunction(funcName, *this, [this]() { origin(); });
}

LL_TYPE_INSTANCE_HOOK(
    ActorPassengerTickHook,
    ll::memory::HookPriority::Normal,
    Actor,
    &Actor::$passengerTick,
    void
) {
    if (!config.enabled || !config.parallelActorPhase ||
        ParallelDimensionTickManager::getInstance().isStopping()) {
        origin();
        return;
    }

    if (tl_replayingActorPhase) {
        origin();
        return;
    }

    if (g_suppressActorTicks.load(std::memory_order_acquire) && !this->isPlayer()) {
        collectActorPhase(*this, ActorPhaseMask::PassengerTick, false);
        return;
    }

    const char* funcName = "Actor::passengerTick";
    ScopedPhase phase(funcName);
    handleActorSensitiveFunction(funcName, *this, [this]() { origin(); });
}

LL_TYPE_INSTANCE_HOOK(
    MobBaseTickHook,
    ll::memory::HookPriority::Normal,
    Mob,
    &Mob::$baseTick,
    void
) {
    if (!config.enabled || !config.parallelActorPhase ||
        ParallelDimensionTickManager::getInstance().isStopping()) {
        origin();
        return;
    }

    if (tl_replayingActorPhase) {
        origin();
        return;
    }

    Actor& actor = static_cast<Actor&>(*this);

    if (g_suppressActorTicks.load(std::memory_order_acquire) && !actor.isPlayer()) {
        collectActorPhase(actor, ActorPhaseMask::BaseTick, true);
        return;
    }

    const char* funcName = "Mob::baseTick";
    ScopedPhase phase(funcName);
    handleActorSensitiveFunction(funcName, actor, [this]() { origin(); });
}

LL_TYPE_INSTANCE_HOOK(
    MobNormalTickHook,
    ll::memory::HookPriority::Normal,
    Mob,
    &Mob::$normalTick,
    void
) {
    if (!config.enabled || !config.parallelActorPhase ||
        ParallelDimensionTickManager::getInstance().isStopping()) {
        origin();
        return;
    }

    if (tl_replayingActorPhase) {
        origin();
        return;
    }

    Actor& actor = static_cast<Actor&>(*this);

    if (g_suppressActorTicks.load(std::memory_order_acquire) && !actor.isPlayer()) {
        collectActorPhase(actor, ActorPhaseMask::NormalTick, true);
        return;
    }

    const char* funcName = "Mob::normalTick";
    ScopedPhase phase(funcName);
    handleActorSensitiveFunction(funcName, actor, [this]() { origin(); });
}

LL_TYPE_INSTANCE_HOOK(
    MobAiStepHook,
    ll::memory::HookPriority::Normal,
    Mob,
    &Mob::$aiStep,
    void
) {
    if (!config.enabled || !config.parallelActorPhase ||
        ParallelDimensionTickManager::getInstance().isStopping()) {
        origin();
        return;
    }

    if (tl_replayingActorPhase) {
        origin();
        return;
    }

    Actor& actor = static_cast<Actor&>(*this);

    if (g_suppressActorTicks.load(std::memory_order_acquire) && !actor.isPlayer()) {
        collectActorPhase(actor, ActorPhaseMask::MobAiStep, true);
        return;
    }

    const char* funcName = "Mob::aiStep";
    ScopedPhase phase(funcName);
    handleActorSensitiveFunction(funcName, actor, [this]() { origin(); });
}

//=============================================================================
// Hooks: player remains main-thread
//=============================================================================

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

//=============================================================================
// Hooks: actor sensitive operations always guarded
//=============================================================================

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
    ActorHandleInsidePortalHook,
    ll::memory::HookPriority::Normal,
    Actor,
    &Actor::$handleInsidePortal,
    void,
    BlockPos const& portalPos
) {
    const char* funcName = "Actor::handleInsidePortal";
    ScopedPhase phase(funcName);
    auto* posPtr = &portalPos;

    handleActorSensitiveFunction(funcName, *this, [this, posPtr]() {
        origin(*posPtr);
    });
}

LL_TYPE_INSTANCE_HOOK(
    ActorChangeDimensionHook,
    ll::memory::HookPriority::Normal,
    Actor,
    &Actor::$changeDimension,
    void,
    DimensionType toId
) {
    const char* funcName = "Actor::changeDimension";
    ScopedPhase phase(funcName);

    handleActorSensitiveFunction(funcName, *this, [this, toId]() {
        origin(toId);
    });
}

LL_TYPE_INSTANCE_HOOK(
    ActorSendMotionPacketIfNeededHook,
    ll::memory::HookPriority::Normal,
    Actor,
    &Actor::$sendMotionPacketIfNeeded,
    void
) {
    const char* funcName = "Actor::sendMotionPacketIfNeeded";
    ScopedPhase phase(funcName);

    handleActorSensitiveFunction(funcName, *this, [this]() {
        origin();
    });
}

//=============================================================================
// Hook: top-level level tick
//=============================================================================

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
    clearCollections();

    auto originStart = std::chrono::steady_clock::now();

    const bool suppressActorPhase     = config.parallelActorPhase;
    const bool suppressDimensionPhase = config.parallelDimensionPhase;

    if (suppressActorPhase && suppressDimensionPhase) {
        ScopedAtomicFlag suppressActors(g_suppressActorTicks, true);
        ScopedAtomicFlag suppressDims(g_suppressDimensionTick, true);
        origin();
    } else if (suppressActorPhase) {
        ScopedAtomicFlag suppressActors(g_suppressActorTicks, true);
        origin();
    } else if (suppressDimensionPhase) {
        ScopedAtomicFlag suppressDims(g_suppressDimensionTick, true);
        origin();
    } else {
        origin();
    }

    auto originEnd = std::chrono::steady_clock::now();

    if (config.parallelActorPhase) {
        auto actorTasks = takeCollectedActors();
        if (!actorTasks.empty()) {
            ParallelDimensionTickManager::getInstance().dispatchActorPhase(this, std::move(actorTasks));
        }
    }

    if (config.parallelDimensionPhase) {
        std::vector<Dimension*> dims;
        {
            std::lock_guard lock(g_collectDimensionMutex);
            dims = std::move(g_collectedDimensions);
        }

        if (!dims.empty()) {
            ParallelDimensionTickManager::getInstance().dispatchDimensionPhase(this, std::move(dims));
        }
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

    logger().info(
        "DimParallel loaded. enabled={} debug={} actorPhase={} dimensionPhase={}",
        config.enabled,
        config.debug,
        config.parallelActorPhase,
        config.parallelDimensionPhase
    );
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
        ActorHandleInsidePortalHook::hook();
        ActorChangeDimensionHook::hook();
        ActorSendMotionPacketIfNeededHook::hook();

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
        ActorSendMotionPacketIfNeededHook::unhook();
        ActorChangeDimensionHook::unhook();
        ActorHandleInsidePortalHook::unhook();
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
