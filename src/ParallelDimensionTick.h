#pragma once

#include <ll/api/Config.h>
#include <ll/api/io/Logger.h>
#include <ll/api/mod/NativeMod.h>

#include <mc/world/level/dimension/Dimension.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

class Actor;
class Player;
class Level;

namespace dim_parallel {

struct Config {
    int  version            = 6;
    bool enabled            = true;
    bool debug              = false;
    bool parallelNormalTick = true;
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

struct LevelTickSnapshot {
    int  time      = 0;
    bool simPaused = false;
};

struct DimensionWorkerContext {
    struct TaskResult {
        std::mutex              mutex;
        std::condition_variable cv;
        bool                    done = false;
        std::exception_ptr      exception;
        bool                    dimensionMismatchBeforeRun = false;
        uint64_t                execTimeUs = 0;
    };

    struct TaskItem {
        std::function<void()>      fn;
        std::shared_ptr<TaskResult> result;
        Actor*                     actor                = nullptr;
        int                        scheduledDimensionId = -1;
        std::string                debugName;
    };

    int dimensionId = -1;

    std::thread             workerThread;
    std::mutex              queueMutex;
    std::condition_variable wakeCV;
    std::deque<TaskItem>    tasks;
    bool                    shutdown = false;

    uint64_t lastTaskTimeUs = 0;
};

class ParallelNormalTickManager {
public:
    static ParallelNormalTickManager& getInstance();

    void initialize();
    void shutdown();

    void beginLevelTick(Level* level);
    void endLevelTick();

    void runActorNormalTick(Actor& actor, const char* funcName, std::function<void()> fn);

    bool isStopping() const;

    static bool                    isWorkerThread();
    static DimensionWorkerContext* getCurrentContext();
    static DimensionType           getCurrentDimensionType();
    static void                    runOnMainThread(std::function<void()> task);

    static void markFunctionDangerous(const std::string& funcName);
    static bool isFunctionDangerous(const std::string& funcName);

    struct Stats {
        std::atomic<uint64_t> totalLevelTicks{0};

        std::atomic<uint64_t> totalNormalTickDispatches{0};
        std::atomic<uint64_t> totalNormalTickTimeUs{0};
        std::atomic<uint64_t> maxNormalTickTimeUs{0};
        std::atomic<uint64_t> totalNormalTickWaitTimeUs{0};
        std::atomic<uint64_t> maxNormalTickWaitTimeUs{0};

        std::atomic<uint64_t> totalMainThreadTasks{0};
        std::atomic<uint64_t> totalMainThreadSyncTasks{0};
        std::atomic<uint64_t> totalMainThreadAsyncTasks{0};

        std::atomic<uint64_t> totalDangerousFunctions{0};

        std::atomic<uint64_t> totalLevelOriginTimeUs{0};
        std::atomic<uint64_t> maxLevelOriginTimeUs{0};

        std::atomic<uint64_t> totalLevelHookTimeUs{0};
        std::atomic<uint64_t> maxLevelHookTimeUs{0};

        std::atomic<uint64_t> totalMainThreadTaskProcessTimeUs{0};
        std::atomic<uint64_t> maxMainThreadTaskProcessTimeUs{0};

        void reset() noexcept {
            totalLevelTicks.store(0, std::memory_order_relaxed);

            totalNormalTickDispatches.store(0, std::memory_order_relaxed);
            totalNormalTickTimeUs.store(0, std::memory_order_relaxed);
            maxNormalTickTimeUs.store(0, std::memory_order_relaxed);
            totalNormalTickWaitTimeUs.store(0, std::memory_order_relaxed);
            maxNormalTickWaitTimeUs.store(0, std::memory_order_relaxed);

            totalMainThreadTasks.store(0, std::memory_order_relaxed);
            totalMainThreadSyncTasks.store(0, std::memory_order_relaxed);
            totalMainThreadAsyncTasks.store(0, std::memory_order_relaxed);

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
    ParallelNormalTickManager() = default;

    struct WindowStats {
        uint64_t levelTicks = 0;

        uint64_t normalTickDispatches = 0;
        uint64_t totalNormalTickTimeUs = 0;
        uint64_t maxNormalTickTimeUs   = 0;
        uint64_t totalNormalTickWaitUs = 0;
        uint64_t maxNormalTickWaitUs   = 0;

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

    DimensionWorkerContext* getOrCreateWorker(int dimensionId);
    void                    workerLoop(DimensionWorkerContext* ctx);
    size_t                  processAllMainThreadTasks();
    void                    maybeLogWindowStats();
    void                    recordNormalTickStats(uint64_t execUs, uint64_t waitUs);

    void     quarantineActor(Actor* actor, uint64_t untilTick);
    bool     isActorQuarantined(Actor* actor, uint64_t nowTick) const;
    void     cleanupActorQuarantine(uint64_t nowTick);

    std::unordered_map<int, std::unique_ptr<DimensionWorkerContext>> mContexts;
    std::mutex                                                       mContextsMutex;

    mutable std::mutex                                               mActorQuarantineMutex;
    std::unordered_map<Actor*, uint64_t>                             mActorMainThreadUntilTick;

    LevelTickSnapshot                                                mSnapshot;
    std::atomic<uint64_t>                                            mCurrentTick{0};
    std::atomic<bool>                                                mStopping{false};
    bool                                                             mInitialized = false;
    bool                                                             mInLevelTick = false;

    Stats                                                            mStats;
    WindowStats                                                      mWindowStats;

    static MainThreadTaskQueue mMainThreadTasks;

    static constexpr uint64_t RECOVERY_INTERVAL_TICKS    = 20;
    static constexpr uint64_t ACTOR_QUARANTINE_TICKS     = 40;
    static constexpr uint64_t DEBUG_WINDOW_TICKS         = 200;
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
