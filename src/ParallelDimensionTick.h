#pragma once
#include <ll/api/Config.h>
#include <ll/api/io/Logger.h>
#include <ll/api/mod/NativeMod.h>
#include <mc/world/level/BlockPos.h>
#include <mc/world/level/dimension/Dimension.h>
#include <mc/network/Packet.h>
#include <atomic>
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

struct LevelTickSnapshot {
    int time = 0;
    bool simPaused = false;
};

// Fiber 上下文：每个维度一个 fiber
struct DimensionFiberContext {
    Dimension* dimensionPtr = nullptr;
    void* fiber = nullptr;           // 维度 fiber
    void* callerFiber = nullptr;     // 调度 fiber（主线程）
    uint64_t lastTickTimeUs = 0;
    bool tickDone = false;
    bool faulted = false;
    DWORD exceptionCode = 0;
    int dimId = -1;
};

class ParallelDimensionTickManager {
public:
    static ParallelDimensionTickManager& getInstance();
    void initialize();
    void shutdown();
    void dispatchAndSync(class Level* level);

    struct Stats {
        std::atomic<uint64_t> totalTicks{0};
        std::atomic<uint64_t> totalFallbackTicks{0};
        std::atomic<uint64_t> totalSEHCaught{0};
        std::atomic<uint64_t> totalSkippedDimensions{0};
        std::atomic<uint64_t> totalRecoveryAttempts{0};
    };
    Stats& getStats() { return mStats; }

private:
    ParallelDimensionTickManager() = default;
    void serialFallbackTick(const std::vector<Dimension*>& dimensions);
    static void CALLBACK fiberProc(LPVOID param);

    std::unordered_map<int, std::unique_ptr<DimensionFiberContext>> mContexts;
    LevelTickSnapshot mSnapshot;
    std::atomic<bool> mFallbackToSerial{false};
    bool mInitialized = false;
    Stats mStats;

    static constexpr uint64_t RECOVERY_INTERVAL_TICKS = 40;
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
