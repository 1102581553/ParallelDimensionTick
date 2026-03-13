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
            try {
                task();
            } catch (...) {}
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

struct DimensionTickResult {
    int dimId = -1;
    uint64_t tickTimeUs = 0;
    bool completed = false;
};

// 流水线模型：主线程串行调用 dim->tick()，但维度之间流水线化
// 维度 A tick 完成后立即开始维度 B tick，同时异步处理维度 A 的后续任务
class ParallelDimensionTickManager {
public:
    static ParallelDimensionTickManager& getInstance();
    void initialize();
    void shutdown();
    void dispatchDimensionTicks(class Level* level);

    static void markFunctionDangerous(const std::string& funcName);
    static bool isFunctionDangerous(const std::string& funcName);

    struct Stats {
        std::atomic<uint64_t> totalTicks{0};
        std::atomic<uint64_t> totalDangerousFunctions{0};
        std::atomic<uint64_t> totalSkippedDimensions{0};
    };
    Stats& getStats() { return mStats; }

private:
    ParallelDimensionTickManager() = default;

    bool mInitialized = false;
    Stats mStats;

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
