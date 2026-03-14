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
#include <utility>

namespace dim_parallel {

static Config                           config;
static std::shared_ptr<ll::io::Logger> log;
static bool                             hookInstalled = false;

static thread_local DimensionWorkerContext* tl_currentContext        = nullptr;
static thread_local bool                    tl_isWorkerThread        = false;
static thread_local bool                    tl_insideNormalTickChain = false;
static thread_local int                     tl_currentDimTypeId      = -1;
static thread_local const char*             tl_currentPhase          = "idle";

// 危险函数恢复：funcName -> recoverAtTick
static std::unordered_map<std::string, uint64_t> g_dangerousFunctions;
static std::mutex                                g_dangerousMutex;

MainThreadTaskQueue ParallelNormalTickManager::mMainThreadTasks;

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

class ScopedNormalTickChain {
public:
    ScopedNormalTickChain() noexcept : mPrev(tl_insideNormalTickChain) { tl_insideNormalTickChain = true; }
    ~ScopedNormalTickChain() noexcept { tl_insideNormalTickChain = mPrev; }

private:
    bool mPrev;
};

template <typename Func>
inline void forwardToMainThread(Func&& func) {
    ParallelNormalTickManager::runOnMainThread(
        [forwarded = std::forward<Func>(func)]() mutable { forwarded(); }
    );
}

inline bool shouldActorRunOnMainThread(Actor& actor) {
    if (!ParallelNormalTickManager::isWorkerThread()) {
        return false;
    }

    if (actor.isPlayer()) {
        return true;
    }

    auto* ctx = ParallelNormalTickManager::getCurrentContext();
    if (ctx == nullptr) {
        return false;
    }

    const auto actorDim = actor.getDimensionId();
    return static_cast<int>(actorDim) != ctx->dimensionId;
}

inline bool shouldPushRunOnMainThread(Actor& self, Actor& other) {
    if (!ParallelNormalTickManager::isWorkerThread()) {
        return false;
    }

    if (self.isPlayer() || other.isPlayer()) {
        return true;
    }

    auto* ctx = ParallelNormalTickManager::getCurrentContext();
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

    if (ParallelNormalTickManager::isWorkerThread() &&
        ParallelNormalTickManager::isFunctionDangerous(funcName)) {
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

    if (ParallelNormalTickManager::isWorkerThread()) {
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

    if (ParallelNormalTickManager::isWorkerThread() &&
        ParallelNormalTickManager::isFunctionDangerous(funcName)) {
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
// ParallelNormalTickManager
//=============================================================================

ParallelNormalTickManager& ParallelNormalTickManager::getInstance() {
    static ParallelNormalTickManager instance;
    return instance;
}

bool ParallelNormalTickManager::isStopping() const {
    return mStopping.load(std::memory_order_acquire);
}

void ParallelNormalTickManager::initialize() {
    if (mInitialized) {
        return;
    }

    mStopping.store(false, std::memory_order_release);
    mCurrentTick.store(0, std::memory_order_release);
    mStats.reset();
    mWindowStats = WindowStats{};
    mInLevelTick = false;
    mInitialized = true;

    logger().info(
        "Initialized normalTick parallel model. enabled={} debugWindow={}",
        config.parallelNormalTick,
        DEBUG_WINDOW_TICKS
    );
}

void ParallelNormalTickManager::shutdown() {
    if (!mInitialized) {
        return;
    }

    mStopping.store(true, std::memory_order_release);

    processAllMainThreadTasks();

    {
        std::lock_guard contextsLock(mContextsMutex);
        for (auto& [id, ctx] : mContexts) {
            if (ctx && ctx->workerThread.joinable()) {
                {
                    std::lock_guard queueLock(ctx->queueMutex);
                    ctx->shutdown = true;
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

    {
        std::lock_guard lock(mActorQuarantineMutex);
        mActorMainThreadUntilTick.clear();
    }

    mCurrentTick.store(0, std::memory_order_release);
    mInitialized = false;
    logger().info("Shutdown");
}

bool ParallelNormalTickManager::isWorkerThread() { return tl_isWorkerThread; }

DimensionWorkerContext* ParallelNormalTickManager::getCurrentContext() { return tl_currentContext; }

DimensionType ParallelNormalTickManager::getCurrentDimensionType() {
    return static_cast<DimensionType>(tl_currentDimTypeId);
}

void ParallelNormalTickManager::runOnMainThread(std::function<void()> task) {
    if (!tl_isWorkerThread || tl_currentContext == nullptr) {
        task();
        return;
    }

    auto sync = mMainThreadTasks.enqueueSync(std::move(task));

    for (;;) {
        {
            std::unique_lock lock(sync->mutex);
            if (sync->cv.wait_for(lock, std::chrono::milliseconds(1), [&sync] { return sync->done; })) {
                break;
            }
        }
    }

    if (sync->exception) {
        std::rethrow_exception(sync->exception);
    }
}

void ParallelNormalTickManager::markFunctionDangerous(const std::string& funcName) {
    auto&    manager   = getInstance();
    uint64_t nowTick   = manager.mCurrentTick.load(std::memory_order_acquire);
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

bool ParallelNormalTickManager::isFunctionDangerous(const std::string& funcName) {
    auto&    manager = getInstance();
    uint64_t nowTick = manager.mCurrentTick.load(std::memory_order_acquire);

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

void ParallelNormalTickManager::quarantineActor(Actor* actor, uint64_t untilTick) {
    if (actor == nullptr) {
        return;
    }

    std::lock_guard lock(mActorQuarantineMutex);
    auto& current = mActorMainThreadUntilTick[actor];
    if (current < untilTick) {
        current = untilTick;
    }
}

bool ParallelNormalTickManager::isActorQuarantined(Actor* actor, uint64_t nowTick) const {
    if (actor == nullptr) {
        return false;
    }

    std::lock_guard lock(mActorQuarantineMutex);
    auto it = mActorMainThreadUntilTick.find(actor);
    return it != mActorMainThreadUntilTick.end() && nowTick < it->second;
}

void ParallelNormalTickManager::cleanupActorQuarantine(uint64_t nowTick) {
    std::lock_guard lock(mActorQuarantineMutex);

    for (auto it = mActorMainThreadUntilTick.begin(); it != mActorMainThreadUntilTick.end();) {
        if (nowTick >= it->second) {
            it = mActorMainThreadUntilTick.erase(it);
        } else {
            ++it;
        }
    }
}

void ParallelNormalTickManager::beginLevelTick(Level* level) {
    if (!mInitialized || !config.parallelNormalTick || !level) {
        mInLevelTick = false;
        return;
    }

    mSnapshot.time      = level->getTime();
    mSnapshot.simPaused = level->getSimPaused();
    mCurrentTick.store(static_cast<uint64_t>(mSnapshot.time), std::memory_order_release);
    cleanupActorQuarantine(static_cast<uint64_t>(mSnapshot.time));
    mInLevelTick = !mSnapshot.simPaused;
}

void ParallelNormalTickManager::endLevelTick() {
    mInLevelTick = false;
    processAllMainThreadTasks();
}

DimensionWorkerContext* ParallelNormalTickManager::getOrCreateWorker(int dimensionId) {
    std::lock_guard lock(mContextsMutex);

    auto it = mContexts.find(dimensionId);
    if (it != mContexts.end() && it->second) {
        return it->second.get();
    }

    auto ctx = std::make_unique<DimensionWorkerContext>();
    auto* raw = ctx.get();
    raw->dimensionId = dimensionId;
    raw->workerThread = std::thread(&ParallelNormalTickManager::workerLoop, this, raw);
    auto [insertIt, inserted] = mContexts.emplace(dimensionId, std::move(ctx));
    if (!inserted || !insertIt->second) {
        if (raw->workerThread.joinable()) {
            {
                std::lock_guard queueLock(raw->queueMutex);
                raw->shutdown = true;
            }
            raw->wakeCV.notify_one();
            raw->workerThread.join();
        }
        return nullptr;
    }
    return insertIt->second.get();
}

void ParallelNormalTickManager::workerLoop(DimensionWorkerContext* ctx) {
    tl_isWorkerThread   = true;
    tl_currentContext   = ctx;
    tl_currentDimTypeId = ctx->dimensionId;
    tl_currentPhase     = "idle";

    for (;;) {
        DimensionWorkerContext::TaskItem task;

        {
            std::unique_lock lock(ctx->queueMutex);
            ctx->wakeCV.wait(lock, [ctx] { return ctx->shutdown || !ctx->tasks.empty(); });

            if (ctx->shutdown && ctx->tasks.empty()) {
                break;
            }

            task = std::move(ctx->tasks.front());
            ctx->tasks.pop_front();
        }

        auto start = std::chrono::steady_clock::now();
        std::exception_ptr taskException;

        try {
            tl_currentPhase = task.debugName.c_str();

            if (task.actor == nullptr ||
                static_cast<int>(task.actor->getDimensionId()) != task.scheduledDimensionId) {
                task.result->dimensionMismatchBeforeRun = true;
            } else {
                task.fn();
            }

            tl_currentPhase = "idle";
        } catch (...) {
            taskException = std::current_exception();
            tl_currentPhase = "idle";
        }

        auto end = std::chrono::steady_clock::now();
        ctx->lastTaskTimeUs = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(end - start).count()
        );

        if (task.result) {
            {
                std::lock_guard resultLock(task.result->mutex);
                task.result->done = true;
                task.result->exception = taskException;
                task.result->execTimeUs = ctx->lastTaskTimeUs;
            }
            task.result->cv.notify_one();
        }
    }

    tl_isWorkerThread   = false;
    tl_currentContext   = nullptr;
    tl_currentDimTypeId = -1;
    tl_currentPhase     = "idle";
}

void ParallelNormalTickManager::runActorNormalTick(Actor& actor, const char* funcName, std::function<void()> fn) {
    ScopedNormalTickChain chain;

    if (!mInitialized || !config.parallelNormalTick || !mInLevelTick || isStopping()) {
        fn();
        return;
    }

    if (actor.isPlayer()) {
        fn();
        return;
    }

    const uint64_t nowTick = mCurrentTick.load(std::memory_order_acquire);

    if (isFunctionDangerous(funcName) || isActorQuarantined(&actor, nowTick)) {
        fn();
        return;
    }

    const int scheduledDim = static_cast<int>(actor.getDimensionId());
    auto* ctx = getOrCreateWorker(scheduledDim);
    if (ctx == nullptr) {
        fn();
        return;
    }

    auto result = std::make_shared<DimensionWorkerContext::TaskResult>();

    {
        std::lock_guard queueLock(ctx->queueMutex);
        DimensionWorkerContext::TaskItem task;
        task.fn = std::move(fn);
        task.result = result;
        task.actor = &actor;
        task.scheduledDimensionId = scheduledDim;
        task.debugName = funcName;
        ctx->tasks.push_back(std::move(task));
    }
    ctx->wakeCV.notify_one();

    auto waitStart = std::chrono::steady_clock::now();

    for (;;) {
        processAllMainThreadTasks();

        {
            std::unique_lock lock(result->mutex);
            if (result->cv.wait_for(lock, std::chrono::milliseconds(1), [&result] { return result->done; })) {
                break;
            }
        }
    }

    auto waitEnd = std::chrono::steady_clock::now();
    const uint64_t waitUs = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(waitEnd - waitStart).count()
    );

    uint64_t execUs = 0;
    bool dimensionMismatchBeforeRun = false;
    std::exception_ptr taskException;

    {
        std::lock_guard lock(result->mutex);
        execUs = result->execTimeUs;
        dimensionMismatchBeforeRun = result->dimensionMismatchBeforeRun;
        taskException = result->exception;
    }

    recordNormalTickStats(execUs, waitUs);

    if (dimensionMismatchBeforeRun) {
        quarantineActor(&actor, nowTick + ACTOR_QUARANTINE_TICKS);
        logger().warn(
            "Actor normalTick dimension mismatch before worker run. func={} scheduledDim={} tick={}",
            funcName,
            scheduledDim,
            nowTick
        );
        return;
    }

    if (taskException) {
        try {
            std::rethrow_exception(taskException);
        } catch (const std::exception& e) {
            logger().error("Exception in {} on dim {}: {}", funcName, scheduledDim, e.what());
        } catch (...) {
            logger().error("Unknown exception in {} on dim {}", funcName, scheduledDim);
        }

        markFunctionDangerous(funcName);
        quarantineActor(&actor, nowTick + ACTOR_QUARANTINE_TICKS);
        return;
    }
}

size_t ParallelNormalTickManager::processAllMainThreadTasks() {
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

void ParallelNormalTickManager::recordNormalTickStats(uint64_t execUs, uint64_t waitUs) {
    mStats.totalNormalTickDispatches.fetch_add(1, std::memory_order_relaxed);
    mStats.totalNormalTickTimeUs.fetch_add(execUs, std::memory_order_relaxed);
    mStats.totalNormalTickWaitTimeUs.fetch_add(waitUs, std::memory_order_relaxed);

    updateMax(mStats.maxNormalTickTimeUs, execUs);
    updateMax(mStats.maxNormalTickWaitTimeUs, waitUs);

    mWindowStats.normalTickDispatches++;
    mWindowStats.totalNormalTickTimeUs += execUs;
    mWindowStats.totalNormalTickWaitUs += waitUs;

    updateMaxValue(mWindowStats.maxNormalTickTimeUs, execUs);
    updateMaxValue(mWindowStats.maxNormalTickWaitUs, waitUs);
}

void ParallelNormalTickManager::maybeLogWindowStats() {
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

    if (window.normalTickDispatches > 0) {
        const uint64_t avgExecUs = window.totalNormalTickTimeUs / window.normalTickDispatches;
        const uint64_t avgWaitUs = window.totalNormalTickWaitUs / window.normalTickDispatches;

        logger().info(
            "  normalTick dispatches={} exec avg={}us max={}us wait avg={}us max={}us",
            window.normalTickDispatches,
            avgExecUs,
            window.maxNormalTickTimeUs,
            avgWaitUs,
            window.maxNormalTickWaitUs
        );
    } else {
        logger().info("  normalTick dispatches=0");
    }

    logger().info(
        "  lifetime: levelTicks={} normalDispatches={} maxLevelHook={}us maxLevelOrigin={}us maxNormalExec={}us maxNormalWait={}us dangerousHits={} mainTasks={} queue={}",
        mStats.totalLevelTicks.load(std::memory_order_relaxed),
        mStats.totalNormalTickDispatches.load(std::memory_order_relaxed),
        mStats.maxLevelHookTimeUs.load(std::memory_order_relaxed),
        mStats.maxLevelOriginTimeUs.load(std::memory_order_relaxed),
        mStats.maxNormalTickTimeUs.load(std::memory_order_relaxed),
        mStats.maxNormalTickWaitTimeUs.load(std::memory_order_relaxed),
        mStats.totalDangerousFunctions.load(std::memory_order_relaxed),
        mStats.totalMainThreadTasks.load(std::memory_order_relaxed),
        mMainThreadTasks.size()
    );
}

void ParallelNormalTickManager::recordLevelTickStats(uint64_t levelOriginUs, uint64_t levelHookUs) {
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
    ActorNormalTickHook,
    ll::memory::HookPriority::Normal,
    Actor,
    &Actor::$normalTick,
    void
) {
    if (!config.enabled || !config.parallelNormalTick ||
        ParallelNormalTickManager::getInstance().isStopping()) {
        origin();
        return;
    }

    if (tl_insideNormalTickChain) {
        origin();
        return;
    }

    if (this->isPlayer()) {
        origin();
        return;
    }

    const char* funcName = "Actor::normalTick";
    ScopedPhase phase(funcName);

    ParallelNormalTickManager::getInstance().runActorNormalTick(
        *this,
        funcName,
        [this]() { origin(); }
    );
}

LL_TYPE_INSTANCE_HOOK(
    MobNormalTickHook,
    ll::memory::HookPriority::Normal,
    Mob,
    &Mob::$normalTick,
    void
) {
    if (!config.enabled || !config.parallelNormalTick ||
        ParallelNormalTickManager::getInstance().isStopping()) {
        origin();
        return;
    }

    if (tl_insideNormalTickChain) {
        origin();
        return;
    }

    Actor& actor = static_cast<Actor&>(*this);

    if (actor.isPlayer()) {
        origin();
        return;
    }

    const char* funcName = "Mob::normalTick";
    ScopedPhase phase(funcName);

    ParallelNormalTickManager::getInstance().runActorNormalTick(
        actor,
        funcName,
        [this]() { origin(); }
    );
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

// 顶层 tick：只负责打开/关闭 normalTick 调度窗口
LL_TYPE_INSTANCE_HOOK(
    LevelTickHook,
    ll::memory::HookPriority::Normal,
    Level,
    &Level::$tick,
    void
) {
    if (!config.enabled || ParallelNormalTickManager::getInstance().isStopping()) {
        origin();
        return;
    }

    auto hookStart   = std::chrono::steady_clock::now();
    auto originStart = std::chrono::steady_clock::now();

    auto& manager = ParallelNormalTickManager::getInstance();
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
        "DimParallel loaded. enabled={} debug={} parallelNormalTick={}",
        config.enabled,
        config.debug,
        config.parallelNormalTick
    );
    return true;
}

bool PluginImpl::enable() {
    if (!hookInstalled) {
        LevelTickHook::hook();

        ActorNormalTickHook::hook();
        MobNormalTickHook::hook();

        PlayerNormalTickHook::hook();
        PlayerAiStepHook::hook();
        PlayerPassengerTickHook::hook();
        PlayerTickWorldHook::hook();

        ActorOnPushHook::hook();

        hookInstalled = true;
    }

    ParallelNormalTickManager::getInstance().initialize();
    logger().info("DimParallel enabled");
    return true;
}

bool PluginImpl::disable() {
    config.enabled = false;
    ParallelNormalTickManager::getInstance().shutdown();

    if (hookInstalled) {
        ActorOnPushHook::unhook();

        PlayerTickWorldHook::unhook();
        PlayerPassengerTickHook::unhook();
        PlayerAiStepHook::unhook();
        PlayerNormalTickHook::unhook();

        MobNormalTickHook::unhook();
        ActorNormalTickHook::unhook();

        LevelTickHook::unhook();

        hookInstalled = false;
    }

    logger().info("DimParallel disabled");
    return true;
}

} // namespace dim_parallel

LL_REGISTER_MOD(dim_parallel::PluginImpl, dim_parallel::PluginImpl::getInstance());
