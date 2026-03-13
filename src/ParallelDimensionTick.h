#pragma once

#include <ll/api/Config.h>
#include <ll/api/io/Logger.h>
#include <ll/api/io/LoggerRegistry.h>
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
#include <queue>

#ifdef _WIN32
#include <windows.h>
#endif

namespace dim_parallel {

struct Config {
    int  version = 1;
    bool enabled = true;
    bool debug   = false;
    
    // 完全隔离配置
    bool enableCompleteIsolation = true;  // 启用完全隔离
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
    int64_t time      = 0;
    bool    simPaused = false;
};

struct DimensionWorkerContext {
    Dimension*          dimension = nullptr;
    MainThreadTaskQueue mainThreadTasks;
    uint64_t            lastTickTimeUs = 0;
    
    // 维度专属线程
    std::thread         workerThread;
    std::queue<std::function<void()>> taskQueue;
    std::mutex          queueMutex;
    std::condition_variable taskCV;      // 任务队列通知
    std::condition_variable completionCV; // 完成通知
    std::atomic<bool>   shouldStop{false};
    std::atomic<bool>   isWorking{false};
    std::atomic<bool>   hasPendingWork{false};
    
    // 线程安全的构造和析构
    DimensionWorkerContext() = default;
    
    // 启动维度专属线程
    void startDimensionThread() {
        if (!workerThread.joinable()) {
            workerThread = std::thread([this]() {
                #ifdef _WIN32
                wchar_t name[64];
                swprintf_s(name, L"Dim_%d_Thread", 
                         this->dimension ? this->dimension->getDimensionId() : -1);
                SetThreadDescription(GetCurrentThread(), name);
                #endif
                
                while (!shouldStop.load(std::memory_order_acquire)) {
                    std::function<void()> task;
                    
                    // 等待任务
                    {
                        std::unique_lock lock(queueMutex);
                        taskCV.wait(lock, [this] { 
                            return hasPendingWork.load(std::memory_order_acquire) || 
                                   shouldStop.load(std::memory_order_acquire); 
                        });
                        
                        if (shouldStop.load(std::memory_order_acquire) && taskQueue.empty()) {
                            break;
                        }
                        
                        if (!taskQueue.empty()) {
                            task = std::move(taskQueue.front());
                            taskQueue.pop();
                            hasPendingWork.store(!taskQueue.empty(), std::memory_order_release);
                        }
                    }
                    
                    // 执行任务
                    if (task) {
                        isWorking.store(true, std::memory_order_release);
                        try {
                            task();
                        } catch (...) {
                            logger().error("Exception in dimension {} thread task", 
                                         dimension ? dimension->getDimensionId() : -1);
                        }
                        isWorking.store(false, std::memory_order_release);
                        
                        // 通知任务完成
                        completionCV.notify_all();
                    }
                }
            });
        }
    }
    
    // 添加tick任务到队列
    void addTickTask(std::function<void()> task) {
        {
            std::lock_guard lock(queueMutex);
            taskQueue.push(std::move(task));
        }
        hasPendingWork.store(true, std::memory_order_release);
        taskCV.notify_one();
    }
    
    // 等待当前任务完成
    void waitForCompletion() {
        std::unique_lock lock(queueMutex);
        completionCV.wait(lock, [this] { 
            return !hasPendingWork.load(std::memory_order_acquire) && 
                   !isWorking.load(std::memory_order_acquire); 
        });
    }
    
    // 关闭线程
    void shutdown() {
        shouldStop.store(true, std::memory_order_release);
        taskCV.notify_all();
        completionCV.notify_all();
        
        if (workerThread.joinable()) {
            workerThread.join();
        }
    }
};

class ParallelDimensionTickManager {
public:
    static ParallelDimensionTickManager& getInstance();

    void initialize();
    void shutdown();
    void dispatchAndSync(class Level* level, std::vector<Dimension*>& dimensions);

    static bool                    isWorkerThread();
    static DimensionWorkerContext* getCurrentContext();
    static int                     getCurrentDimensionType();
    static void                    runOnMainThread(std::function<void()> task);

    struct Stats {
        std::atomic<uint64_t> totalParallelTicks{0};
        std::atomic<uint64_t> totalFallbackTicks{0};
        std::atomic<uint64_t> totalMainThreadTasks{0};
        std::atomic<uint64_t> maxDimTickTimeUs{0};
    };
    Stats& getStats() { return mStats; }

private:
    ParallelDimensionTickManager() = default;

    void tickDimensionAsync(DimensionWorkerContext& ctx);
    void processAllMainThreadTasks();
    void serialFallbackTick(std::vector<Dimension*>& dimensions);
    void handleTickException(int dimId);
    bool shouldRecoverFromFallback(); // 检查是否应该恢复

    std::unordered_map<int, std::unique_ptr<DimensionWorkerContext>> mContexts;
    LevelTickSnapshot                               mSnapshot;
    std::atomic<bool>                               mFallbackToSerial{false};
    std::atomic<int64_t>                            mLastFallbackGameTime{0};
    std::atomic<int64_t>                            mLastSuccessfulParallelTick{0}; // 最后一次成功并行tick的时间
    static constexpr int64_t                        RECOVERY_DELAY = 200;
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
