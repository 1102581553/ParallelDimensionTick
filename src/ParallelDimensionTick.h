#pragma once

#include <ll/api/Config.h>
#include <ll/api/io/Logger.h>
#include <ll/api/mod/NativeMod.h>

#include <atomic>
#include <functional>
#include <latch>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include <mc/world/level/dimension/Dimension.h>

namespace dim_parallel {

// ==================== 配置 ====================

struct Config {
    int  version = 1;
    bool enabled = true;
    bool debug   = false;
};

Config&        getConfig();
bool           loadConfig();
bool           saveConfig();
ll::io::Logger& logger();

// ==================== 线程安全任务队列 ====================

class MainThreadTaskQueue {
public:
    void enqueue(std::function<void()> task);
    void processAll();
    size_t size() const;

private:
    mutable std::mutex              mMutex;
    std::vector<std::function<void()>> mTasks;
    std::vector<std::function<void()>> mProcessing; // 双缓冲，减少锁持有时间
};

// ==================== 数据包缓冲 ====================

struct PacketEntry {
    enum class Type {
        Broadcast,
        ForPosition,
        ForEntity
    };

    Type                                        type;
    std::string                                 packetData; // 序列化的包数据
    std::shared_ptr<class Packet>               packet;     // 原始包指针（如果可以安全共享）
    class Player const*                         except = nullptr;

    // ForPosition
    BlockPos                                    position;

    // ForEntity - 存 actor runtime id 而非指针，避免悬垂
    int64_t                                     actorRuntimeId = 0;
};

class DimensionPacketBuffer {
public:
    void addBroadcast(class Packet const& packet, class Player* except);
    void addForPosition(BlockPos const& pos, class Packet const& packet, class Player const* except);
    void addForEntity(class Actor const& actor, class Packet const& packet, class Player const* except);

    void flushTo(Dimension& dim);
    void clear();
    size_t size() const;

private:
    std::vector<PacketEntry> mEntries;
};

// ==================== Level 状态快照 ====================

struct LevelTickSnapshot {
    int      time       = 0;
    uint64_t currentTick = 0;
    bool     simPaused  = false;
};

// ==================== 维度工作线程上下文 ====================

struct DimensionWorkerContext {
    Dimension*            dimension = nullptr;
    DimensionPacketBuffer packetBuffer;
    MainThreadTaskQueue   mainThreadTasks;
    uint64_t              lastTickTimeUs = 0; // 性能统计
};

// ==================== 并行调度管理器 ====================

class ParallelDimensionTickManager {
public:
    static ParallelDimensionTickManager& getInstance();

    void initialize();
    void shutdown();

    // 主线程调用：收集维度、并行 tick、同步
    void dispatchAndSync(class Level* level);

    // 工作线程查询当前上下文
    static bool                   isWorkerThread();
    static DimensionWorkerContext* getCurrentContext();
    static DimensionType           getCurrentDimensionType();

    // 主线程任务提交（从工作线程调用）
    static void runOnMainThread(std::function<void()> task);

    // 统计
    struct Stats {
        std::atomic<uint64_t> totalParallelTicks{0};
        std::atomic<uint64_t> totalFallbackTicks{0};
        std::atomic<uint64_t> totalMainThreadTasks{0};
        std::atomic<uint64_t> totalPacketsBuffered{0};
        std::atomic<uint64_t> maxDimTickTimeUs{0};
    };
    Stats const& getStats() const { return mStats; }

private:
    ParallelDimensionTickManager() = default;

    void tickDimensionOnWorker(DimensionWorkerContext& ctx);
    void flushAllPacketBuffers();
    void processAllMainThreadTasks();
    void serialFallbackTick(std::vector<Dimension*>& dimensions);

    std::unordered_map<int, DimensionWorkerContext> mContexts; // key = DimensionType id
    LevelTickSnapshot                               mSnapshot;
    std::atomic<bool>                               mFallbackToSerial{false};
    std::atomic<bool>                               mShutdownRequested{false};
    bool                                            mInitialized = false;

    Stats mStats;
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
