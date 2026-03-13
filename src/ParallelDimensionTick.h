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
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <string>
#include <Windows.h>

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

//=============================================================================
// 主线程任务队列
//=============================================================================

class MainThreadTaskQueue {
public:
    MainThreadTaskQueue() : mWriteBuffer(0) {}

    void enqueue(std::function<void()> task) {
        int writeIdx = mWriteBuffer.load(std::memory_order_relaxed);
        std::lock_guard lock(mBufferMutex[writeIdx]);
        mBuffers[writeIdx].push_back(std::move(task));
    }

    void processAll() {
        int readIdx = mWriteBuffer.load(std::memory_order_relaxed);
        int newWrite = 1 - readIdx;
        mWriteBuffer.store(newWrite, std::memory_order_release);

        std::vector<std::function<void()>> tasks;
        {
            std::lock_guard lock(mBufferMutex[readIdx]);
            tasks.swap(mBuffers[readIdx]);
        }
        for (auto& task : tasks) {
            try { task(); } catch (...) {}
        }
    }

    size_t size() const {
        size_t total = 0;
        for (int i = 0; i < 2; ++i) {
            std::lock_guard lock(mBufferMutex[i]);
            total += mBuffers[i].size();
        }
        return total;
    }

private:
    std::atomic<int> mWriteBuffer;
    mutable std::mutex mBufferMutex[2];
    std::vector<std::function<void()>> mBuffers[2];
};

//=============================================================================
// 维度 Fiber 上下文
//=============================================================================

struct DimensionFiberContext {
    Dimension* dimensionPtr = nullptr;
    void* dimFiber = nullptr;          // 维度 tick fiber
    uint64_t lastTickTimeUs = 0;
    bool tickDone = false;
    bool faulted = false;
    DWORD exceptionCode = 0;
    int dimId = -1;
    MainThreadTaskQueue mainThreadTasks;

    // 工作线程相关
    HANDLE workerThread = nullptr;
    std::mutex wakeMutex;
    std::condition_variable wakeCV;
    std::atomic<bool> shouldWork{false};
    std::atomic<bool> shutdown{false};
    std::atomic<bool> tickCompleted{false};
    std::atomic<bool> isProcessing{false};
    std::atomic<uint64_t> tickNumber{0};
    std::atomic<uint64_t> skippedTicks{0};
    std::atomic<uint64_t> totalSkippedTicks{0};
};

//=============================================================================
// 管理器
//=============================================================================

class ParallelDimensionTickManager {
public:
    static ParallelDimensionTickManager& getInstance();
    void initialize();
    void shutdown();
    void dispatchAndSync(class Level* level);

    static bool isWorkerThread();
    static DimensionFiberContext* getCurrentContext();
    static void runOnMainThread(std::function<void()> task);

    static void markFunctionDangerous(const std::string& funcName);
    static bool isFunctionDangerous(const std::string& funcName);

    struct Stats {
        std::atomic<uint64_t> totalParallelTicks{0};
        std::atomic<uint64_t> totalFallbackTicks{0};
        std::atomic<uint64_t> totalMainThreadTasks{0};
        std::atomic<uint64_t> totalSEHCaught{0};
        std::atomic<uint64_t> totalSkippedDimensions{0};
        std::atomic<uint64_t> totalRecoveryAttempts{0};
        std::atomic<uint64_t> totalTicksSkippedDueToBacklog{0};
        std::atomic<uint64_t> cycleMainThreadTasks{0};
        std::atomic<uint64_t> totalDangerousFunctions{0};
    };
    Stats& getStats() { return mStats; }

private:
    ParallelDimensionTickManager() = default;
    void serialFallbackTick(const std::vector<Dimension*>& dimensions);
    static void CALLBACK dimFiberProc(LPVOID param);
    static DWORD WINAPI workerThreadProc(LPVOID param);

    std::unordered_map<int, std::unique_ptr<DimensionFiberContext>> mContexts;
    std::atomic<bool> mFallbackToSerial{false};
    bool mInitialized = false;
    Stats mStats;
    int mWorkerCount = 0;

    static constexpr uint64_t RECOVERY_INTERVAL_TICKS = 40;
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
