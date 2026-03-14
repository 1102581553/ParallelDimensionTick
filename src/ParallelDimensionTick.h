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

namespace dim_parallel {

struct Config {
    int  version = 1;
    bool enabled = true;
    bool debug   = false;
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
    Dimension*              dimension      = nullptr;
    uint64_t                lastTickTimeUs = 0;

    std::thread             workerThread;
    std::mutex              wakeMutex;
    std::condition_variable wakeCV;
    bool                    shouldWork = false;
    bool                    shutdown   = false;
    std::atomic<bool>       tickCompleted{false};
};

class ParallelDimensionTickManager {
public:
    static ParallelDimensionTickManager& getInstance();

    void initialize();
    void shutdown();
    void dispatchAndSync(class Level* level, std::vector<Dimension*> dimensions);

    bool isStopping() const;

    static bool                    isWorkerThread();
    static DimensionWorkerContext* getCurrentContext();
    static DimensionType           getCurrentDimensionType();
    static void                    runOnMainThread(std::function<void()> task);

    static void markFunctionDangerous(const std::string& funcName);
    static bool isFunctionDangerous(const std::string& funcName);

    struct Stats {
        std::atomic<uint64_t> totalLevelTicks{0};
        std::atomic<uint64_t> totalParallelTicks{0};
        std::atomic<uint64_t> totalFallbackTicks{0};

        std::atomic<uint64_t> totalMainThreadTasks{0};
        std::atomic<uint64_t> totalMainThreadSyncTasks{0};
        std::atomic<uint64_t> totalMainThreadAsyncTasks{0};

        std::atomic<uint64_t> totalRecoveryAttempts{0};
        std::atomic<uint64_t> totalDangerousFunctions{0};

        std::atomic<uint64_t> totalLevelOriginTimeUs{0};
        std::atomic<uint64_t> maxLevelOriginTimeUs{0};

        std::atomic<uint64_t> totalLevelHookTimeUs{0};
        std::atomic<uint64_t> maxLevelHookTimeUs{0};

        std::atomic<uint64_t> totalDispatchTimeUs{0};
        std::atomic<uint64_t> maxDispatchTimeUs{0};

        std::atomic<uint64_t> totalDispatchWaitTimeUs{0};
        std::atomic<uint64_t> maxDispatchWaitTimeUs{0};

        std::atomic<uint64_t> totalAllDimTickTimeUs{0};
        std::atomic<uint64_t> maxAllDimTickTimeUs{0};
        std::atomic<uint64_t> maxDimTickTimeUs{0};

        std::atomic<uint64_t> totalFallbackTimeUs{0};
        std::atomic<uint64_t> maxFallbackTimeUs{0};

        std::atomic<uint64_t> totalMainThreadTaskProcessTimeUs{0};
        std::atomic<uint64_t> maxMainThreadTaskProcessTimeUs{0};

        void reset() noexcept {
            totalLevelTicks.store(0, std::memory_order_relaxed);
            totalParallelTicks.store(0, std::memory_order_relaxed);
            totalFallbackTicks.store(0, std::memory_order_relaxed);

            totalMainThreadTasks.store(0, std::memory_order_relaxed);
            totalMainThreadSyncTasks.store(0, std::memory_order_relaxed);
            totalMainThreadAsyncTasks.store(0, std::memory_order_relaxed);

            totalRecoveryAttempts.store(0, std::memory_order_relaxed);
            totalDangerousFunctions.store(0, std::memory_order_relaxed);

            totalLevelOriginTimeUs.store(0, std::memory_order_relaxed);
            maxLevelOriginTimeUs.store(0, std::memory_order_relaxed);

            totalLevelHookTimeUs.store(0, std::memory_order_relaxed);
            maxLevelHookTimeUs.store(0, std::memory_order_relaxed);

            totalDispatchTimeUs.store(0, std::memory_order_relaxed);
            maxDispatchTimeUs.store(0, std::memory_order_relaxed);

            totalDispatchWaitTimeUs.store(0, std::memory_order_relaxed);
            maxDispatchWaitTimeUs.store(0, std::memory_order_relaxed);

            totalAllDimTickTimeUs.store(0, std::memory_order_relaxed);
            maxAllDimTickTimeUs.store(0, std::memory_order_relaxed);
            maxDimTickTimeUs.store(0, std::memory_order_relaxed);

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
        uint64_t parallelTicks = 0;
        uint64_t fallbackTicks = 0;

        uint64_t totalLevelOriginTimeUs = 0;
        uint64_t maxLevelOriginTimeUs   = 0;

        uint64_t totalLevelHookTimeUs = 0;
        uint64_t maxLevelHookTimeUs   = 0;

        uint64_t totalDispatchTimeUs = 0;
        uint64_t maxDispatchTimeUs   = 0;

        uint64_t totalDispatchWaitTimeUs = 0;
        uint64_t maxDispatchWaitTimeUs   = 0;

        uint64_t totalAllDimTickTimeUs = 0;
        uint64_t maxAllDimTickTimeUs   = 0;

        uint64_t totalFallbackTimeUs = 0;
        uint64_t maxFallbackTimeUs   = 0;

        uint64_t totalMainThreadTasks = 0;
        uint64_t totalMainThreadSyncTasks = 0;
        uint64_t totalMainThreadAsyncTasks = 0;

        uint64_t totalMainThreadTaskProcessTimeUs = 0;
        uint64_t maxMainThreadTaskProcessTimeUs   = 0;

        uint64_t totalDispatchDims = 0;
        uint64_t maxDispatchDims   = 0;
    };

    void     tickDimensionOnWorker(DimensionWorkerContext& ctx);
    size_t   processAllMainThreadTasks();
    void     serialFallbackTick(const std::vector<Dimension*>& dimensions);
    void     workerLoop(DimensionWorkerContext* ctx);
    void     notifyDispatchProgress();
    uint64_t getCurrentTick() const;

    void recordDispatchStats(uint64_t dispatchUs, uint64_t waitUs, uint64_t allDimUs, size_t dims);
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
