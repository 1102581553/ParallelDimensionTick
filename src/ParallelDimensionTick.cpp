#include "ParallelDimensionTick.h"
#include <ll/api/memory/Hook.h>
#include <ll/api/mod/RegisterHelper.h>
#include <ll/api/io/LoggerRegistry.h>
#include <mc/world/level/Level.h>
#include <mc/world/level/dimension/Dimension.h>
#include <mc/world/actor/Actor.h>
#include <mc/network/Packet.h>
#include <chrono>
#include <filesystem>
#include <unordered_set>

namespace dim_parallel {

static Config config;
static std::shared_ptr<ll::io::Logger> log;
static bool hookInstalled = false;

static thread_local DimensionWorkerContext* tl_currentContext = nullptr;
static thread_local bool tl_isWorkerThread = false;
static thread_local int tl_currentDimTypeId = -1;
static thread_local const char* tl_currentPhase = "idle";

static std::atomic<bool> g_inParallelPhase{false};
static std::atomic<bool> g_suppressDimensionTick{false};

// 存储本 tick 内有效的 Dimension*
static std::vector<Dimension*> g_collectedDimensions;

Config& getConfig() { return config; }

ll::io::Logger& logger() {
    if (!log) log = ll::io::LoggerRegistry::getInstance().getOrCreate("DimParallel");
    return *log;
}

bool loadConfig() {
    auto path = PluginImpl::getInstance().getSelf().getConfigDir() / "config.json";
    return ll::config::loadConfig(config, path);
}
bool saveConfig() {
    auto path = PluginImpl::getInstance().getSelf().getConfigDir() / "config.json";
    return ll::config::saveConfig(config, path);
}

// MainThreadTaskQueue 实现
void MainThreadTaskQueue::enqueue(std::function<void()> task) {
    std::lock_guard lock(mMutex);
    mTasks.push_back(std::move(task));
}
void MainThreadTaskQueue::processAll() {
    {
        std::lock_guard lock(mMutex);
        mProcessing.swap(mTasks);
    }
    for (auto& task : mProcessing) task();
    mProcessing.clear();
}
size_t MainThreadTaskQueue::size() const {
    std::lock_guard lock(mMutex);
    return mTasks.size();
}

//=============================================================================
// ParallelDimensionTickManager 实现（已修复）
//=============================================================================
ParallelDimensionTickManager& ParallelDimensionTickManager::getInstance() {
    static ParallelDimensionTickManager instance;
    return instance;
}

void ParallelDimensionTickManager::initialize() { /* 同上一次 */ }
void ParallelDimensionTickManager::shutdown() { /* 同上一次 */ }
bool ParallelDimensionTickManager::isWorkerThread() { return tl_isWorkerThread; }
DimensionWorkerContext* ParallelDimensionTickManager::getCurrentContext() { return tl_currentContext; }
DimensionType ParallelDimensionTickManager::getCurrentDimensionType() { return DimensionType(tl_currentDimTypeId); }
void ParallelDimensionTickManager::runOnMainThread(std::function<void()> task) { /* 同上一次 */ }

// 危险函数标记（静态实现）
void ParallelDimensionTickManager::markFunctionDangerous(const std::string& funcName) {
    static std::unordered_set<std::string> g_dangerousFunctions;
    static std::mutex g_dangerousMutex;
    std::lock_guard lock(g_dangerousMutex);
    if (g_dangerousFunctions.insert(funcName).second) {
        logger().warn("Function '{}' marked as dangerous (will run on main thread)", funcName);
        getInstance().mStats.totalDangerousFunctions++;
    }
}
bool ParallelDimensionTickManager::isFunctionDangerous(const std::string& funcName) {
    static std::unordered_set<std::string> g_dangerousFunctions;
    static std::mutex g_dangerousMutex;
    std::lock_guard lock(g_dangerousMutex);
    return g_dangerousFunctions.find(funcName) != g_dangerousFunctions.end();
}

void ParallelDimensionTickManager::workerLoop(DimensionWorkerContext* ctx) { /* 同上一次 */ }
void ParallelDimensionTickManager::dispatchAndSync(Level* level) { /* 同上一次 */ }
void ParallelDimensionTickManager::tickDimensionOnWorker(DimensionWorkerContext& ctx) { /* 同上一次 */ }
void ParallelDimensionTickManager::serialFallbackTick(const std::vector<Dimension*>& dimRefs) { /* 同上一次 */ }

// 危险函数处理模板
template<typename Func, typename... Args>
inline void handleDangerousFunction(const char* funcName, Func&& func, Args&&... args) {
    if (!config.enabled) {
        std::forward<Func>(func)(std::forward<Args>(args)...);
        return;
    }
    if (ParallelDimensionTickManager::isWorkerThread() &&
        ParallelDimensionTickManager::isFunctionDangerous(funcName)) {
        auto bound = std::bind(std::forward<Func>(func), std::forward<Args>(args)...);
        ParallelDimensionTickManager::runOnMainThread([bound = std::move(bound)]() mutable { bound(); });
        return;
    }
    std::forward<Func>(func)(std::forward<Args>(args)...);
}

//=============================================================================
// ====================== 全部 Hook（已完整展开） ======================
//=============================================================================

// 1. Level 主 Hook（收集维度）
LL_TYPE_INSTANCE_HOOK(
    LevelTickHook,
    ll::memory::HookPriority::Normal,
    Level,
    &Level::$tick,
    void
) {
    static thread_local bool inHook = false;
    if (!config.enabled || inHook) {
        origin();
        return;
    }
    inHook = true;

    g_collectedDimensions.clear();
    g_suppressDimensionTick.store(true, std::memory_order_release);

    this->forEachDimension([&](Dimension& dim) -> bool {
        g_collectedDimensions.emplace_back(&dim);
        return true;
    });

    origin();
    g_suppressDimensionTick.store(false, std::memory_order_release);

    if (!g_collectedDimensions.empty()) {
        g_inParallelPhase.store(true, std::memory_order_release);
        ParallelDimensionTickManager::getInstance().dispatchAndSync(this);
        g_inParallelPhase.store(false, std::memory_order_release);
    }
    inHook = false;
}

// 2. Dimension 主 Hook（仅抑制）
LL_TYPE_INSTANCE_HOOK(
    DimensionTickHook,
    ll::memory::HookPriority::Normal,
    Dimension,
    &Dimension::$tick,
    void
) {
    static thread_local bool inHook = false;
    if (!config.enabled) { origin(); return; }
    if (g_suppressDimensionTick.load(std::memory_order_acquire)) return;
    if (g_inParallelPhase.load(std::memory_order_acquire) || inHook) {
        origin();
        return;
    }
    inHook = true;
    origin();
    inHook = false;
}

// ====================== 以下为 8 个子 Hook（完整） ======================

// 3. tickRedstone
LL_TYPE_INSTANCE_HOOK(
    DimensionTickRedstoneHook,
    ll::memory::HookPriority::Normal,
    Dimension,
    &Dimension::$tickRedstone,
    void
) {
    const char* funcName = "tickRedstone";
    tl_currentPhase = funcName;
    handleDangerousFunction(funcName, [this]() { origin(); });
}

// 4. _sendBlocksChangedPackets
LL_TYPE_INSTANCE_HOOK(
    DimensionSendBlocksChangedHook,
    ll::memory::HookPriority::Normal,
    Dimension,
    &Dimension::_sendBlocksChangedPackets,
    void
) {
    const char* funcName = "_sendBlocksChangedPackets";
    tl_currentPhase = funcName;
    handleDangerousFunction(funcName, [this]() { origin(); });
}

// 5. _processEntityChunkTransfers
LL_TYPE_INSTANCE_HOOK(
    DimensionProcessEntityTransfersHook,
    ll::memory::HookPriority::Normal,
    Dimension,
    &Dimension::_processEntityChunkTransfers,
    void
) {
    const char* funcName = "_processEntityChunkTransfers";
    tl_currentPhase = funcName;
    handleDangerousFunction(funcName, [this]() { origin(); });
}

// 6. _tickEntityChunkMoves
LL_TYPE_INSTANCE_HOOK(
    DimensionTickEntityChunkMovesHook,
    ll::memory::HookPriority::Normal,
    Dimension,
    &Dimension::_tickEntityChunkMoves,
    void
) {
    const char* funcName = "_tickEntityChunkMoves";
    tl_currentPhase = funcName;
    handleDangerousFunction(funcName, [this]() { origin(); });
}

// 7. _runChunkGenerationWatchdog
LL_TYPE_INSTANCE_HOOK(
    DimensionRunChunkGenWatchdogHook,
    ll::memory::HookPriority::Normal,
    Dimension,
    &Dimension::_runChunkGenerationWatchdog,
    void
) {
    const char* funcName = "_runChunkGenerationWatchdog";
    tl_currentPhase = funcName;
    handleDangerousFunction(funcName, [this]() { origin(); });
}

// 8. $sendBroadcast
LL_TYPE_INSTANCE_HOOK(
    DimensionSendBroadcastHook,
    ll::memory::HookPriority::Normal,
    Dimension,
    &Dimension::$sendBroadcast,
    void,
    Packet const& packet,
    Player* except
) {
    const char* funcName = "sendBroadcast";
    tl_currentPhase = funcName;
    handleDangerousFunction(funcName, [this, &packet, except]() { origin(packet, except); });
}

// 9. $sendPacketForPosition
LL_TYPE_INSTANCE_HOOK(
    DimensionSendPacketForPositionHook,
    ll::memory::HookPriority::Normal,
    Dimension,
    &Dimension::$sendPacketForPosition,
    void,
    BlockPos const& position,
    Packet const& packet,
    Player const* except
) {
    const char* funcName = "sendPacketForPosition";
    tl_currentPhase = funcName;
    handleDangerousFunction(funcName, [this, &position, &packet, except]() { origin(position, packet, except); });
}

// 10. $sendPacketForEntity
LL_TYPE_INSTANCE_HOOK(
    DimensionSendPacketForEntityHook,
    ll::memory::HookPriority::Normal,
    Dimension,
    &Dimension::$sendPacketForEntity,
    void,
    Actor const& actor,
    Packet const& packet,
    Player const* except
) {
    const char* funcName = "sendPacketForEntity";
    tl_currentPhase = funcName;
    handleDangerousFunction(funcName, [this, &actor, &packet, except]() { origin(actor, packet, except); });
}

//=============================================================================
// PluginImpl（保持不变）
//=============================================================================
PluginImpl& PluginImpl::getInstance() {
    static PluginImpl instance;
    return instance;
}

bool PluginImpl::load() {
    std::filesystem::create_directories(getSelf().getConfigDir());
    if (!loadConfig()) {
        logger().warn("Failed to load config, using defaults");
        saveConfig();
    }
    logger().info("DimParallel loaded. enabled={} debug={}", config.enabled, config.debug);
    return true;
}

bool PluginImpl::enable() {
    if (!hookInstalled) {
        LevelTickHook::hook();
        DimensionTickHook::hook();
        DimensionTickRedstoneHook::hook();
        DimensionSendBlocksChangedHook::hook();
        DimensionProcessEntityTransfersHook::hook();
        DimensionTickEntityChunkMovesHook::hook();
        DimensionRunChunkGenWatchdogHook::hook();
        DimensionSendBroadcastHook::hook();
        DimensionSendPacketForPositionHook::hook();
        DimensionSendPacketForEntityHook::hook();
        hookInstalled = true;
    }
    ParallelDimensionTickManager::getInstance().initialize();
    logger().info("DimParallel enabled");
    return true;
}

bool PluginImpl::disable() {
    ParallelDimensionTickManager::getInstance().shutdown();
    if (hookInstalled) {
        LevelTickHook::unhook();
        DimensionTickHook::unhook();
        DimensionTickRedstoneHook::unhook();
        DimensionSendBlocksChangedHook::unhook();
        DimensionProcessEntityTransfersHook::unhook();
        DimensionTickEntityChunkMovesHook::unhook();
        DimensionRunChunkGenWatchdogHook::unhook();
        DimensionSendBroadcastHook::unhook();
        DimensionSendPacketForPositionHook::unhook();
        DimensionSendPacketForEntityHook::unhook();
        hookInstalled = false;
    }
    logger().info("DimParallel disabled");
    return true;
}

} // namespace dim_parallel

LL_REGISTER_MOD(dim_parallel::PluginImpl, dim_parallel::PluginImpl::getInstance());
