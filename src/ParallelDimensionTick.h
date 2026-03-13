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
#include <unordered_set>
#include <vector>

namespace dim_parallel {

struct Config {
    int version = 1;
    bool enabled = true;
    bool debug = false;
};

Config& getConfig();
bool loadConfig();
bool saveConfig();
ll::io::Logger& logger();

class MainThreadTaskQueue {
public:
    void enqueue(std::function<void()> task);
    void processAll();
    size_t size() const;

private:
    mutable std::mutex mMutex;
    std::vector<std::function<void()>> mTasks;
    std::vector<std::function<void()>> mProcessing;
};

struct LevelTickSnapshot {
    int time = 0;
    bool simPaused = false;
};

struct DimensionWorkerContext {
    Dimension* dimensionPtr = nullptr;
    uint64_t lastTickTimeUs = 0;
    MainThreadTaskQueue mainThreadTasks;
    std::thread workerThread;
    std::mutex wakeMutex;
    std::condition_variable wakeCV;
    bool shouldWork = false;
    bool shutdown = false;
    std::atomic<bool> tickCompleted{false};
    std::atomic<bool> isProcessing{false};
    uint64_t tickNumber = 0;
    uint64_t skippedTicks = 0;
    uint64_t totalSkippedTicks = 0;
};

class ParallelDimensionTickManager {
public:
    static ParallelDimensionTickManager& getInstance();
    void initialize();
    void shutdown();
    void dispatchAndSync(class Level* level);
    static bool isWorkerThread();
    static DimensionWorkerContext* getCurrentContext();
    static DimensionType getCurrentDimensionType();
    static void runOnMainThread(std::function<void()> task);

    static void markFunctionDangerous(const std::string& funcName);
    static bool isFunctionDangerous(const std::string& funcName);

    struct Stats {
        std::atomic<uint64_t> totalParallelTicks{0};
        std::atomic<uint64_t> totalFallbackTicks{0};
        std::atomic<uint64_t> totalMainThreadTasks{0};
        std::atomic<uint64_t> maxDimTickTimeUs{0};
        std::atomic<uint64_t> totalRecoveryAttempts{0};
        std::atomic<uint64_t> totalDangerousFunctions{0};
        std::atomic<uint64_t> totalSkippedDimensions{0};
        std::atomic<uint64_t> cycleMainThreadTasks{0};
        std::atomic<uint64_t> totalTicksSkippedDueToBacklog{0};
    };
    Stats& getStats() { return mStats; }

private:
    ParallelDimensionTickManager() = default;
    void tickDimensionOnWorker(DimensionWorkerContext& ctx);
    void serialFallbackTick(const std::vector<Dimension*>& dimensions);
    void workerLoop(DimensionWorkerContext* ctx);

    std::unordered_map<int, std::unique_ptr<DimensionWorkerContext>> mContexts;
    LevelTickSnapshot mSnapshot;
    std::atomic<bool> mFallbackToSerial{false};
    bool mInitialized = false;
    Stats mStats;

    static constexpr uint64_t RECOVERY_INTERVAL_TICKS = 20;
    uint64_t mFallbackStartTick = 0;

    static std::unordered_set<std::string> m_dangerousFunctions;
    static std::mutex m_dangerousMutex;
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
