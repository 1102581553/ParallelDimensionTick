#pragma once

#include <ll/api/Config.h>
#include <ll/api/io/Logger.h>
#include <ll/api/mod/NativeMod.h>

#include <mc/world/level/BlockPos.h>
#include <mc/world/level/dimension/Dimension.h>
#include <mc/network/Packet.h>

#include <atomic>
#include <functional>
#include <latch>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

namespace dim_parallel {

// ==================== 配置 ====================

struct Config {
    int  version = 1;
    bool enabled = true;
    bool debug   = false;
};

Config&         getConfig();
bool            loadConfig();
bool            saveConfig();
ll::io::Logger& logger();

// ==================== 线程安全任务队列 ====================

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

// ==================== Level 状态快照 ====================

struct LevelTickSnapshot {
    int      time        = 0;
    uint64_t currentTick = 0;
    bool     simPaused   = false;
};

// ==================== 维度工作线程上下文 ====================

struct DimensionWorkerContext {
    Dimension*          dimension = nullptr;
    MainThreadTaskQueue mainThreadTasks;
    uint64_t            lastTickTimeUs = 0;
};

// ==================== 并行调度管理器 ====================

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

    struct Stats {
        std::atomic<uint64_t> totalParallelTicks{0};
        std::atomic<uint64_t> totalFallbackTicks{0};
        std::atomic<uint64_t> totalMainThreadTasks{0};
        std::atomic<uint64_t> maxDimTickTimeUs{0};
    };
    Stats const& getStats() const { return mStats; }

private:
    ParallelDimensionTickManager() = default;

    void tickDimensionOnWorker(DimensionWorkerContext& ctx);
    void processAllMainThreadTasks();
    void serialFallbackTick(std::vector<Dimension*>& dimensions);

    std::unordered_map<int, DimensionWorkerContext> mContexts;
    LevelTickSnapshot                               mSnapshot;
    std::atomic<bool>                               mFallbackToSerial{false};
    std::atomic<bool>                               mShutdownRequested{false};
    bool                                            mInitialized = false;
    Stats                                           mStats;
};

// ==================== 插件主类 ====================

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
