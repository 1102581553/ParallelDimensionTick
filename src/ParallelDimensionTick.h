#pragma once

#include <ll/api/Config.h>
#include <ll/api/io/Logger.h>
#include <ll/api/mod/NativeMod.h>

#include <mc/world/level/BlockPos.h>
#include <mc/world/level/dimension/Dimension.h>
#include <mc/network/Packet.h>

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

namespace dim_parallel {

struct Config {
    int  version           = 1;
    bool enabled           = true;
    bool debug             = false;
    bool parallelChunkTick = true;
};

Config&         getConfig();
bool            loadConfig();
bool            saveConfig();
ll::io::Logger& logger();

class MainThreadTaskQueue {
public:
    void   enqueue(std::function<void()> task);
    void   processAll();
    size_t size() const;

private:
    mutable std::mutex                 mMutex;
    std::vector<std::function<void()>> mTasks;
    std::vector<std::function<void()>> mProcessing;
};

struct LevelTickSnapshot {
    int  time      = 0;
    bool simPaused = false;
};

struct DimensionWorkerContext {
    Dimension*          dimension = nullptr;
    MainThreadTaskQueue mainThreadTasks;
    uint64_t            lastTickTimeUs = 0;
};

class WorkerPool {
public:
    void start(int numWorkers);
    void stop();
    void executeAll(std::vector<std::function<void()>>& tasks);
    int  workerCount() const { return static_cast<int>(mHandles.size()); }

private:
    static unsigned long __stdcall threadEntry(void* param);
    void workerLoop();

    struct Batch {
        std::function<void()>* tasks    = nullptr;
        int                    count    = 0;
        std::atomic<int>       nextIdx{0};
        std::atomic<int>       doneCount{0};
    };

    std::vector<void*>       mHandles; // HANDLE
    std::mutex               mMutex;
    std::condition_variable  mWakeCV;
    Batch                    mBatch;
    uint64_t                 mGeneration = 0;
    bool                     mShutdown   = false;
};


class ParallelDimensionTickManager {
public:
    static ParallelDimensionTickManager& getInstance();

    void initialize();
    void shutdown();
    void dispatchAndSync(class Level* level);

    static bool                    isWorkerThread();
    static DimensionWorkerContext* getCurrentContext();
    static DimensionType           getCurrentDimensionType();
    static void                    runOnMainThread(std::function<void()> task);

    WorkerPool& getWorkerPool() { return mPool; }

    struct Stats {
        std::atomic<uint64_t> totalParallelTicks{0};
        std::atomic<uint64_t> totalFallbackTicks{0};
        std::atomic<uint64_t> totalMainThreadTasks{0};
        std::atomic<uint64_t> maxDimTickTimeUs{0};
        std::atomic<uint64_t> chunksTickedParallel{0};
        std::atomic<uint64_t> chunkBatchCount{0};
    };
    Stats& getStats() { return mStats; }

private:
    ParallelDimensionTickManager() = default;

    void tickDimensionOnWorker(DimensionWorkerContext& ctx);
    void processAllMainThreadTasks();
    void serialFallbackTick(std::vector<Dimension*>& dimensions);

    std::unordered_map<int, DimensionWorkerContext> mContexts;
    LevelTickSnapshot                               mSnapshot;
    WorkerPool                                      mPool;
    std::atomic<bool>                               mFallbackToSerial{false};
    bool                                            mInitialized = false;
    Stats                                           mStats;
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
