#pragma once

#include <ll/api/Config.h>
#include <ll/api/io/Logger.h>
#include <ll/api/mod/NativeMod.h>

#include <mc/world/level/dimension/Dimension.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

class Actor;
class Mob;
class Player;
class Level;

namespace dim_parallel {

struct Config {
    int  version            = 4;
    bool enabled            = true;
    bool debug              = false;
    bool parallelActorPhase = true;
};

Config&         getConfig();
bool            loadConfig();
bool            saveConfig();
ll::io::Logger& logger();

class MainThreadTaskQueue {
public:
    struct SyncState {
        std::mutex              mutex;
        std::condition_variable cv;
        bool                    done = false;
        std::exception_ptr      exception;
    };

    struct ProcessStats {
        size_t   total     = 0;
        size_t   sync      = 0;
        size_t   async     = 0;
        uint64_t elapsedUs = 0;
    };

    void                       enqueue(std::function<void()> task);
    std::shared_ptr<SyncState> enqueueSync(std::function<void()> task);
    ProcessStats               processAll();
    size_t                     size() const;

private:
    struct TaskItem {
        std::function<void()>      fn;
        std::shared_ptr<SyncState> sync;
    };

    mutable std::mutex    mMutex;
    std::vector<TaskItem> mTasks;
    std::vector<TaskItem> mProcessing;
};

enum class ActorTickPhase : uint8_t {
    None = 0,
    BaseTick,
    NormalTick,
    AiStep,
    PassengerTick,
};

struct ActorWorkItem {
    Actor* actor = nullptr;
    bool   isMob = false;
};

struct LevelTickSnapshot {
    int  time      = 0;
    bool simPaused = false;
};

struct DimensionWorkerContext {
    int                        dimensionId = -1;
    ActorTickPhase             phase       = ActorTickPhase::None;
    std::vector<ActorWorkItem> actorTasks;

    uint64_t lastWorkTimeUs   = 0;
    uint64_t lastActorCount   = 0;

    std::thread             workerThread;
    std::mutex              wakeMutex;
    std::condition_variable wakeCV;
    bool                    shouldWork = false;
    bool                    shutdown   = false;
    std::atomic<bool>       jobCompleted{false};
};

class ParallelActorTickManager {
public:
    static ParallelActorTickManager& getInstance();

    void initialize();
    void shutdown();

    void beginLevelTick(Level* level);
    void endLevelTick();

    bool interceptActorPhase(Actor& actor, ActorTickPhase phase, bool isMob);

    bool isStopping() const;

    static bool                    isWorkerThread();
    static DimensionWorkerContext* getCurrentContext();
    static DimensionType           getCurrentDimensionType();
    static void                    runOnMainThread(std::function<void()> task);

    static void markFunctionDangerous(const std::string& funcName);
    static bool isFunctionDangerous(const std::string& funcName);

    struct Stats {
        std::atomic<uint64_t> totalLevelTicks{0};

        std::atomic<uint64_t> totalActorDispatches{0};
        std::atomic<uint64_t> totalActorBatches{0};
        std::atomic<uint64_t> totalActorDispatchTimeUs{0};
        std::atomic<uint64_t> maxActorDispatchTimeUs{0};
        std::atomic<uint64_t> totalActorWaitTimeUs{0};
        std::atomic<uint64_t> maxActorWaitTimeUs{0};
        std::atomic<uint64_t> totalActorWorkTimeUs{0};
        std::atomic<uint64_t> maxActorWorkTimeUs{0};
        std::atomic<uint64_t> maxSingleDimActorTimeUs{0};

        std::atomic<uint64_t> totalMainThreadTasks{0};
        std::atomic<uint64_t> totalMainThreadSyncTasks{0};
        std::atomic<uint64_t> totalMainThreadAsyncTasks{0};

        std::atomic<uint64_t> totalRecoveryAttempts{0};
        std::atomic<uint64_t> totalDangerousFunctions{0};

        std::atomic<uint64_t> totalLevelOriginTimeUs{0};
        std::atomic<uint64_t> maxLevelOriginTimeUs{0};

        std::atomic<uint64_t> totalLevelHookTimeUs{0};
        std::atomic<uint64_t> maxLevelHookTimeUs{0};

        std::atomic<uint64_t> totalMainThreadTaskProcessTimeUs{0};
        std::atomic<uint64_t> maxMainThreadTaskProcessTimeUs{0};

        void reset() noexcept {
            totalLevelTicks.store(0, std::memory_order_relaxed);

            totalActorDispatches.store(0, std::memory_order_relaxed);
            totalActorBatches.store(0, std::memory_order_relaxed);
            totalActorDispatchTimeUs.store(0, std::memory_order_relaxed);
            maxActorDispatchTimeUs.store(0, std::memory_order_relaxed);
            totalActorWaitTimeUs.store(0, std::memory_order_relaxed);
            maxActorWaitTimeUs.store(0, std::memory_order_relaxed);
            totalActorWorkTimeUs.store(0, std::memory_order_relaxed);
            maxActorWorkTimeUs.store(0, std::memory_order_relaxed);
            maxSingleDimActorTimeUs.store(0, std::memory_order_relaxed);

            totalMainThreadTasks.store(0, std::memory_order_relaxed);
            totalMainThreadSyncTasks.store(0, std::memory_order_relaxed);
            totalMainThreadAsyncTasks.store(0, std::memory_order_relaxed);

            totalRecoveryAttempts.store(0, std::memory_order_relaxed);
            totalDangerousFunctions.store(0, std::memory_order_relaxed);

            totalLevelOriginTimeUs.store(0, std::memory_order_relaxed);
            maxLevelOriginTimeUs.store(0, std::memory_order_relaxed);

            totalLevelHookTimeUs.store(0, std::memory_order_relaxed);
            maxLevelHookTimeUs.store(0, std::memory_order_relaxed);

            totalMainThreadTaskProcessTimeUs.store(0, std::memory_order_relaxed);
            maxMainThreadTaskProcessTimeUs.store(0, std::memory_order_relaxed);
        }
    };

    Stats& getStats() { return mStats; }

    void recordLevelTickStats(uint64_t levelOriginUs, uint64_t levelHookUs);

private:
    ParallelActorTickManager() = default;

    struct CollectedPhaseBucket {
        std::vector<ActorWorkItem>         items;
        std::unordered_map<Actor*, size_t> index;
    };

    struct WindowStats {
        uint64_t levelTicks = 0;

        uint64_t actorDispatches = 0;
        uint64_t actorBatches    = 0;
        uint64_t totalActorDispatchTimeUs = 0;
        uint64_t maxActorDispatchTimeUs   = 0;
        uint64_t totalActorWaitTimeUs     = 0;
        uint64_t maxActorWaitTimeUs       = 0;
        uint64_t totalActorWorkTimeUs     = 0;
        uint64_t maxActorWorkTimeUs       = 0;

        uint64_t totalLevelOriginTimeUs = 0;
        uint64_t maxLevelOriginTimeUs   = 0;

        uint64_t totalLevelHookTimeUs = 0;
        uint64_t maxLevelHookTimeUs   = 0;

        uint64_t totalMainThreadTasks = 0;
        uint64_t totalMainThreadSyncTasks = 0;
        uint64_t totalMainThreadAsyncTasks = 0;

        uint64_t totalMainThreadTaskProcessTimeUs = 0;
        uint64_t maxMainThreadTaskProcessTimeUs   = 0;
    };

    void     workerLoop(DimensionWorkerContext* ctx);
    void     processActorPhaseOnWorker(DimensionWorkerContext& ctx);
    size_t   processAllMainThreadTasks();
    void     flushCurrentPhase();
    void     clearCurrentPhaseBuckets();
    void     collectActor(Actor& actor, bool isMob);
    void     notifyDispatchProgress();
    uint64_t getCurrentTick() const;
    void     maybeLogWindowStats();

    void recordActorPhaseStats(
        uint64_t dispatchUs,
        uint64_t waitUs,
        uint64_t workUs,
        uint64_t actorCount
    );

    static int phaseOrder(ActorTickPhase phase);
    static const char* phaseName(ActorTickPhase phase);

    std::unordered_map<int, std::unique_ptr<DimensionWorkerContext>> mContexts;
    std::mutex                                                       mContextsMutex;

    std::unordered_map<int, CollectedPhaseBucket>                    mCurrentPhaseBuckets;

    LevelTickSnapshot                                                mSnapshot;
    std::atomic<uint64_t>                                            mCurrentTick{0};
    std::atomic<bool>                                                mStopping{false};
    std::atomic<uint32_t>                                            mActiveDispatches{0};
    bool                                                             mInitialized = false;
    bool                                                             mInLevelTick = false;
    bool                                                             mBypassCurrentTick = false;
    ActorTickPhase                                                   mCurrentPhase = ActorTickPhase::None;

    Stats                                                            mStats;
    WindowStats                                                      mWindowStats;

    std::mutex              mDispatchMutex;
    std::condition_variable mDispatchCV;

    static MainThreadTaskQueue mMainThreadTasks;

    static constexpr uint64_t RECOVERY_INTERVAL_TICKS = 20;
    static constexpr uint64_t DEBUG_WINDOW_TICKS      = 200;
};

class PluginImpl {
public:
    static PluginImpl& getInstance();
    PluginImpl() : mSelf(*ll::mod::NativeMod::current()) {}
    [[nodiscard]] ll::mod::NativeMod& getSelf() const { return mSelf; }

    bool load();
    bool enable();
    bool disable();

private:
    ll::mod::NativeMod& mSelf;
};

} // namespace dim_parallel
