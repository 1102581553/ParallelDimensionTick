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

    void                       enqueue(std::function<void()> task);
    std::shared_ptr<SyncState> enqueueSync(std::function<void()> task);
    size_t                     processAll();
    size_t                     size() const;

private:
    struct TaskItem {
        std::function<void()>      fn;
        std::shared_ptr<SyncState> sync;
    };

    mutable std::mutex     mMutex;
    std::vector<TaskItem>  mTasks;
    std::vector<TaskItem>  mProcessing;
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
        std::atomic<uint64_t> totalParallelTicks{0};
        std::atomic<uint64_t> totalFallbackTicks{0};
        std::atomic<uint64_t> totalMainThreadTasks{0};
        std::atomic<uint64_t> maxDimTickTimeUs{0};
        std::atomic<uint64_t> totalRecoveryAttempts{0};
        std::atomic<uint64_t> totalDangerousFunctions{0};
        std::atomic<uint64_t> totalDangerousRecoveries{0};
    };

    Stats& getStats() { return mStats; }

private:
    ParallelDimensionTickManager() = default;

    void   tickDimensionOnWorker(DimensionWorkerContext& ctx);
    size_t processAllMainThreadTasks();
    void   serialFallbackTick(const std::vector<Dimension*>& dimensions);
    void   workerLoop(DimensionWorkerContext* ctx);
    void   notifyDispatchProgress();
    void   recoverDangerousFunctions(uint64_t currentTick);

    std::unordered_map<int, std::unique_ptr<DimensionWorkerContext>> mContexts;
    std::mutex                                                       mContextsMutex;

    LevelTickSnapshot                                                mSnapshot;
    std::atomic<bool>                                                mFallbackToSerial{false};
    std::atomic<bool>                                                mStopping{false};
    std::atomic<uint32_t>                                            mActiveDispatches{0};
    std::atomic<uint64_t>                                            mCurrentTick{0};
    bool                                                             mInitialized = false;
    Stats                                                            mStats;

    std::mutex              mDispatchMutex;
    std::condition_variable mDispatchCV;

    static MainThreadTaskQueue mMainThreadTasks;

    static constexpr uint64_t RECOVERY_INTERVAL_TICKS            = 20;
    static constexpr uint64_t DANGEROUS_FUNCTION_RECOVERY_TICKS  = 20;
    uint64_t                  mFallbackStartTick                 = 0;
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
