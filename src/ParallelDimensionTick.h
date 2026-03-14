#pragma once

#include <ll/api/Config.h>
#include <ll/api/io/Logger.h>
#include <ll/api/mod/NativeMod.h>

#include <mc/network/Packet.h>
#include <mc/world/level/BlockPos.h>
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
    int  version                = 2;
    bool enabled                = true;
    bool debug                  = false;
    bool parallelActorPhase     = true;
    bool parallelDimensionPhase = true;
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

enum class ActorPhaseMask : uint32_t {
    None          = 0,
    BaseTick      = 1u << 0,
    NormalTick    = 1u << 1,
    PassengerTick = 1u << 2,
    MobAiStep     = 1u << 3,
};

constexpr ActorPhaseMask operator|(ActorPhaseMask a, ActorPhaseMask b) noexcept {
    return static_cast<ActorPhaseMask>(
        static_cast<uint32_t>(a) | static_cast<uint32_t>(b)
    );
}

constexpr ActorPhaseMask operator&(ActorPhaseMask a, ActorPhaseMask b) noexcept {
    return static_cast<ActorPhaseMask>(
        static_cast<uint32_t>(a) & static_cast<uint32_t>(b)
    );
}

constexpr ActorPhaseMask& operator|=(ActorPhaseMask& a, ActorPhaseMask b) noexcept {
    a = a | b;
    return a;
}

struct ActorWorkItem {
    Actor*         actor     = nullptr;
    ActorPhaseMask phaseMask = ActorPhaseMask::None;
    bool           isMob     = false;
};

struct LevelTickSnapshot {
    int  time      = 0;
    bool simPaused = false;
};

struct DimensionWorkerContext {
    enum class JobType : uint8_t {
        None = 0,
        ActorPhase,
        DimensionPhase,
    };

    Dimension*              dimension = nullptr;
    int                     dimensionId = -1;

    std::vector<ActorWorkItem> actorTasks;
    JobType                    jobType = JobType::None;

    uint64_t lastActorPhaseTimeUs     = 0;
    uint64_t lastDimensionTickTimeUs  = 0;
    uint64_t lastActorPhaseCallCount  = 0;
    uint64_t lastActorCount           = 0;

    std::thread             workerThread;
    std::mutex              wakeMutex;
    std::condition_variable wakeCV;
    bool                    shouldWork = false;
    bool                    shutdown   = false;
    std::atomic<bool>       jobCompleted{false};
};

class ParallelDimensionTickManager {
public:
    static ParallelDimensionTickManager& getInstance();

    void initialize();
    void shutdown();

    void dispatchActorPhase(
        Level* level,
        std::unordered_map<int, std::vector<ActorWorkItem>> tasksByDim
    );
    void dispatchDimensionPhase(Level* level, std::vector<Dimension*> dimensions);

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
        std::atomic<uint64_t> totalActorPhaseCalls{0};
        std::atomic<uint64_t> totalActorDispatchTimeUs{0};
        std::atomic<uint64_t> maxActorDispatchTimeUs{0};
        std::atomic<uint64_t> totalActorWaitTimeUs{0};
        std::atomic<uint64_t> maxActorWaitTimeUs{0};
        std::atomic<uint64_t> totalActorWorkTimeUs{0};
        std::atomic<uint64_t> maxActorWorkTimeUs{0};

        std::atomic<uint64_t> totalParallelDimensionTicks{0};
        std::atomic<uint64_t> totalFallbackDimensionTicks{0};
        std::atomic<uint64_t> totalDimensionDispatchTimeUs{0};
        std::atomic<uint64_t> maxDimensionDispatchTimeUs{0};
        std::atomic<uint64_t> totalDimensionWaitTimeUs{0};
        std::atomic<uint64_t> maxDimensionWaitTimeUs{0};
        std::atomic<uint64_t> totalAllDimTickTimeUs{0};
        std::atomic<uint64_t> maxAllDimTickTimeUs{0};
        std::atomic<uint64_t> maxSingleDimTickTimeUs{0};

        std::atomic<uint64_t> totalMainThreadTasks{0};
        std::atomic<uint64_t> totalMainThreadSyncTasks{0};
        std::atomic<uint64_t> totalMainThreadAsyncTasks{0};

        std::atomic<uint64_t> totalRecoveryAttempts{0};
        std::atomic<uint64_t> totalDangerousFunctions{0};

        std::atomic<uint64_t> totalLevelOriginTimeUs{0};
        std::atomic<uint64_t> maxLevelOriginTimeUs{0};

        std::atomic<uint64_t> totalLevelHookTimeUs{0};
        std::atomic<uint64_t> maxLevelHookTimeUs{0};

        std::atomic<uint64_t> totalFallbackTimeUs{0};
        std::atomic<uint64_t> maxFallbackTimeUs{0};

        std::atomic<uint64_t> totalMainThreadTaskProcessTimeUs{0};
        std::atomic<uint64_t> maxMainThreadTaskProcessTimeUs{0};

        void reset() noexcept {
            totalLevelTicks.store(0, std::memory_order_relaxed);

            totalActorDispatches.store(0, std::memory_order_relaxed);
            totalActorBatches.store(0, std::memory_order_relaxed);
            totalActorPhaseCalls.store(0, std::memory_order_relaxed);
            totalActorDispatchTimeUs.store(0, std::memory_order_relaxed);
            maxActorDispatchTimeUs.store(0, std::memory_order_relaxed);
            totalActorWaitTimeUs.store(0, std::memory_order_relaxed);
            maxActorWaitTimeUs.store(0, std::memory_order_relaxed);
            totalActorWorkTimeUs.store(0, std::memory_order_relaxed);
            maxActorWorkTimeUs.store(0, std::memory_order_relaxed);

            totalParallelDimensionTicks.store(0, std::memory_order_relaxed);
            totalFallbackDimensionTicks.store(0, std::memory_order_relaxed);
            totalDimensionDispatchTimeUs.store(0, std::memory_order_relaxed);
            maxDimensionDispatchTimeUs.store(0, std::memory_order_relaxed);
            totalDimensionWaitTimeUs.store(0, std::memory_order_relaxed);
            maxDimensionWaitTimeUs.store(0, std::memory_order_relaxed);
            totalAllDimTickTimeUs.store(0, std::memory_order_relaxed);
            maxAllDimTickTimeUs.store(0, std::memory_order_relaxed);
            maxSingleDimTickTimeUs.store(0, std::memory_order_relaxed);

            totalMainThreadTasks.store(0, std::memory_order_relaxed);
            totalMainThreadSyncTasks.store(0, std::memory_order_relaxed);
            totalMainThreadAsyncTasks.store(0, std::memory_order_relaxed);

            totalRecoveryAttempts.store(0, std::memory_order_relaxed);
            totalDangerousFunctions.store(0, std::memory_order_relaxed);

            totalLevelOriginTimeUs.store(0, std::memory_order_relaxed);
            maxLevelOriginTimeUs.store(0, std::memory_order_relaxed);

            totalLevelHookTimeUs.store(0, std::memory_order_relaxed);
            maxLevelHookTimeUs.store(0, std::memory_order_relaxed);

            totalFallbackTimeUs.store(0, std::memory_order_relaxed);
            maxFallbackTimeUs.store(0, std::memory_order_relaxed);

            totalMainThreadTaskProcessTimeUs.store(0, std::memory_order_relaxed);
            maxMainThreadTaskProcessTimeUs.store(0, std::memory_order_relaxed);
        }
    };

    Stats& getStats() { return mStats; }

    void recordLevelTickStats(uint64_t levelOriginUs, uint64_t levelHookUs);

private:
    ParallelDimensionTickManager() = default;

    struct WindowStats {
        uint64_t levelTicks = 0;

        uint64_t actorDispatches = 0;
        uint64_t actorBatches    = 0;
        uint64_t actorPhaseCalls = 0;
        uint64_t totalActorDispatchTimeUs = 0;
        uint64_t maxActorDispatchTimeUs   = 0;
        uint64_t totalActorWaitTimeUs     = 0;
        uint64_t maxActorWaitTimeUs       = 0;
        uint64_t totalActorWorkTimeUs     = 0;
        uint64_t maxActorWorkTimeUs       = 0;

        uint64_t parallelDimensionTicks = 0;
        uint64_t fallbackDimensionTicks = 0;
        uint64_t totalDimensionDispatchTimeUs = 0;
        uint64_t maxDimensionDispatchTimeUs   = 0;
        uint64_t totalDimensionWaitTimeUs     = 0;
        uint64_t maxDimensionWaitTimeUs       = 0;
        uint64_t totalAllDimTickTimeUs        = 0;
        uint64_t maxAllDimTickTimeUs          = 0;
        uint64_t totalDispatchDims            = 0;
        uint64_t maxDispatchDims              = 0;

        uint64_t totalLevelOriginTimeUs = 0;
        uint64_t maxLevelOriginTimeUs   = 0;

        uint64_t totalLevelHookTimeUs = 0;
        uint64_t maxLevelHookTimeUs   = 0;

        uint64_t totalFallbackTimeUs = 0;
        uint64_t maxFallbackTimeUs   = 0;

        uint64_t totalMainThreadTasks = 0;
        uint64_t totalMainThreadSyncTasks = 0;
        uint64_t totalMainThreadAsyncTasks = 0;

        uint64_t totalMainThreadTaskProcessTimeUs = 0;
        uint64_t maxMainThreadTaskProcessTimeUs   = 0;
    };

    void     workerLoop(DimensionWorkerContext* ctx);
    void     processActorPhaseOnWorker(DimensionWorkerContext& ctx);
    void     tickDimensionOnWorker(DimensionWorkerContext& ctx);
    size_t   processAllMainThreadTasks();
    void     serialFallbackDimensionTick(const std::vector<Dimension*>& dimensions);
    void     notifyDispatchProgress();
    uint64_t getCurrentTick() const;

    void recordActorPhaseStats(
        uint64_t dispatchUs,
        uint64_t waitUs,
        uint64_t workUs,
        uint64_t actorCount,
        uint64_t phaseCalls
    );
    void recordDimensionPhaseStats(
        uint64_t dispatchUs,
        uint64_t waitUs,
        uint64_t allDimUs,
        size_t   dims
    );
    void recordFallbackStats(uint64_t fallbackUs);
    void maybeLogWindowStats();

    std::unordered_map<int, std::unique_ptr<DimensionWorkerContext>> mContexts;
    std::mutex                                                       mContextsMutex;

    LevelTickSnapshot                                                mSnapshot;
    std::atomic<uint64_t>                                            mCurrentTick{0};
    std::atomic<bool>                                                mFallbackToSerial{false};
    std::atomic<bool>                                                mStopping{false};
    std::atomic<uint32_t>                                            mActiveDispatches{0};
    bool                                                             mInitialized = false;
    Stats                                                            mStats;
    WindowStats                                                      mWindowStats;

    std::mutex              mDispatchMutex;
    std::condition_variable mDispatchCV;

    static MainThreadTaskQueue mMainThreadTasks;

    static constexpr uint64_t RECOVERY_INTERVAL_TICKS = 20;
    static constexpr uint64_t DEBUG_WINDOW_TICKS      = 200;

    uint64_t mFallbackStartTick = 0;
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
