package com.github.xingjianluo.otafleetrolloutjava.tests;

import com.github.xingjianluo.otafleetrolloutjava.config.OtaProperties;
import com.github.xingjianluo.otafleetrolloutjava.model.Device;
import com.github.xingjianluo.otafleetrolloutjava.model.DevicePhase;
import com.github.xingjianluo.otafleetrolloutjava.model.DeviceStatus;
import com.github.xingjianluo.otafleetrolloutjava.model.FailureReason;
import com.github.xingjianluo.otafleetrolloutjava.model.Rollout;
import com.github.xingjianluo.otafleetrolloutjava.model.RolloutState;
import com.github.xingjianluo.otafleetrolloutjava.service.DeviceService;
import com.github.xingjianluo.otafleetrolloutjava.service.RolloutService;
import com.github.xingjianluo.otafleetrolloutjava.store.InMemoryStore;
import com.github.xingjianluo.otafleetrolloutjava.tests.support.MutableClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 失败重试、回滚确定性、离线设备重新上线后版本收敛。
 */
class RollbackAndReconnectTest {

    private static final Logger log = LoggerFactory.getLogger(RollbackAndReconnectTest.class);

    private MutableClock clock;
    private InMemoryStore store;
    private RolloutService rolloutService;
    private DeviceService deviceService;
    private OtaProperties props;

    @BeforeEach
    void setUp() {
        clock = new MutableClock();
        store = new InMemoryStore();
        rolloutService = new RolloutService(store, clock);
        props = new OtaProperties();
        props.setOfflineThresholdMillis(10_000);
        deviceService = new DeviceService(store, clock, props, rolloutService);
    }

    private void registerDevices(List<String> ids, String version) {
        ids.forEach(id -> deviceService.register(id, version));
    }

    private void dispatchAll(Rollout r, List<String> devices) {
        devices.forEach(d -> rolloutService.dispatchUpgrade(r.getRolloutId(), null, d));
    }

    private void success(Rollout r, String d, String v) {
        rolloutService.reportResult(r.getRolloutId(), d, true, v, null);
    }

    private void failure(Rollout r, String d, String err) {
        rolloutService.reportResult(r.getRolloutId(), d, false, null, err);
    }

    @Test
    @DisplayName("失败设备重试：暂停后重试成功，resume 推进批次，版本与状态一致")
    void retryFailedDeviceThenResumeAdvances() {
        List<String> b0 = List.of("d0", "d1");
        List<String> b1 = List.of("d2");
        registerDevices(b0, "v1");
        registerDevices(b1, "v1");
        Rollout r = rolloutService.create("r-retry", "v2", "v1",
                List.of(b0, b1), 1.0d, 0.0d, 3);
        rolloutService.start(r.getRolloutId());
        dispatchAll(r, b0);

        success(r, "d0", "v2");
        failure(r, "d1", "flash-error");
        Rollout paused = store.findRollout("r-retry").orElseThrow();
        log.info("[测试输入] d0 成功、d1 失败（failureThreshold=0）；[判定依据] 任一失败即暂停；[最终状态] state={}, d1Phase={}, reason={}",
                paused.getState(), paused.getDeviceStates().get("d1").getPhase(), paused.getFailureReason());
        assertEquals(RolloutState.PAUSED, paused.getState());

        // 重试 d1 并成功
        rolloutService.retryFailed(r.getRolloutId(), null, "d1");
        success(r, "d1", "v2");
        // 暂停不会自动解除，必须显式 resume
        assertEquals(RolloutState.PAUSED, store.findRollout("r-retry").orElseThrow().getState());
        rolloutService.resume(r.getRolloutId());
        Rollout advanced = store.findRollout("r-retry").orElseThrow();
        log.info("[测试输入] 重试 d1 成功后 resume；[判定依据] 失败计数修正为0，成功率100%达标；[最终状态] state={}, currentBatch={}, d1版本={}",
                advanced.getState(), advanced.getCurrentBatch(),
                store.findDevice("d1").map(Device::getCurrentVersion).orElseThrow());
        assertEquals(RolloutState.RUNNING, advanced.getState());
        assertEquals(1, advanced.getCurrentBatch());
        assertEquals(0, advanced.getBatchProgress().get(0).getFailed());
        assertEquals("v2", store.findDevice("d1").orElseThrow().getCurrentVersion());
        assertEquals(DevicePhase.SUCCESS, advanced.getDeviceStates().get("d1").getPhase());
    }

    @Test
    @DisplayName("回滚确定性：成功/失败/在途设备全部回到唯一基线版本，任务 ROLLED_BACK")
    void rollbackConvergesAllDevicesToSingleBaseVersion() {
        List<String> b0 = List.of("ok", "bad", "pending");
        registerDevices(b0, "v1");
        Rollout r = rolloutService.create("r-rb", "v2", "v1",
                List.of(b0, List.of("future")), 0.9d, 0.2d, 3);
        rolloutService.start(r.getRolloutId());
        dispatchAll(r, b0);

        success(r, "ok", "v2");           // 已成功，版本 v2
        failure(r, "bad", "err");         // 失败，版本仍 v1
        // pending 保持 UPGRADING（设备一直离线，未上报）
        clock.advanceMillis(30_000);
        assertEquals(DeviceStatus.OFFLINE, deviceService.status("pending"));

        Rollout rolling = rolloutService.rollback(r.getRolloutId(), null);
        log.info("[测试输入] 批次内 成功x1 失败x1 在途x1，触发回滚；[判定依据] 已打开批次中所有非IDLE设备均回滚；[最终状态] state={}, rollbackTotal={}",
                rolling.getState(), rolling.getRollbackTotal());
        assertEquals(RolloutState.ROLLING_BACK, rolling.getState());
        assertEquals(3, rolling.getRollbackTotal());

        // 回滚结果上报（重复上报幂等）
        rolloutService.reportResult(r.getRolloutId(), "ok", true, "v1", null);
        rolloutService.reportResult(r.getRolloutId(), "ok", true, "v1", null);
        rolloutService.reportResult(r.getRolloutId(), "bad", true, "v1", null);
        rolloutService.reportResult(r.getRolloutId(), "pending", true, "v1", null);

        Rollout done = store.findRollout("r-rb").orElseThrow();
        Set<String> versions = b0.stream()
                .map(id -> store.findDevice(id).orElseThrow().getCurrentVersion())
                .collect(Collectors.toSet());
        log.info("[测试输入] 3 台设备回滚结果均成功（含重复上报）；[判定依据] 唯一允许版本=base v1；[最终状态] state={}, 全量版本集合={}, 阶段={}/{}/{}",
                done.getState(), versions,
                done.getDeviceStates().get("ok").getPhase(),
                done.getDeviceStates().get("bad").getPhase(),
                done.getDeviceStates().get("pending").getPhase());
        assertEquals(RolloutState.ROLLED_BACK, done.getState());
        assertEquals(Set.of("v1"), versions, "回滚后生效版本必须唯一且等于基线版本");
        b0.forEach(id -> assertEquals(DevicePhase.ROLLED_BACK,
                done.getDeviceStates().get(id).getPhase()));
        // 回滚前已因失败率暂停，回滚必须保留可追溯的原始失败原因
        assertEquals(FailureReason.FAILURE_RATE_EXCEEDED, done.getFailureReason());
        assertTrue(done.getFailureDetail() != null && !done.getFailureDetail().isBlank());
    }

    @Test
    @DisplayName("离线设备升级命令丢失，重新上线后心跳触发重新下发并收敛到目标版本")
    void offlineDeviceReconnectsAndConvergesToTarget() {
        List<String> b0 = List.of("online-1", "offline-1");
        registerDevices(b0, "v1");
        Rollout r = rolloutService.create("r-reconnect", "v2", "v1",
                List.of(b0), 0.9d, 0.5d, 3);
        rolloutService.start(r.getRolloutId());
        dispatchAll(r, b0);

        // 离线设备在命令下发后长时间离线，并模拟命令在离线期间丢失
        clock.advanceMillis(30_000);
        assertEquals(DeviceStatus.OFFLINE, deviceService.status("offline-1"));
        store.removeCommandsForDevice("offline-1");
        DeviceTaskPhaseAsserts.assertUpgrading(r, "offline-1");

        // 在线设备正常升级成功
        success(r, "online-1", "v2");

        // 离线设备重新上线：心跳即收敛触发点
        deviceService.heartbeat("offline-1", null);
        boolean commandReissued = store.findCommand("cmd-up-r-reconnect-offline-1-a0").isPresent();
        log.info("[测试输入] offline-1 离线30s且升级命令丢失，随后重新上线心跳；[判定依据] 中间态UPGRADING且无在途命令->重发确定性命令；[最终状态] 命令已重发={}",
                commandReissued);
        assertTrue(commandReissued, "上线后必须重新下发升级命令");

        // 设备随后上报成功，版本收敛到 v2，无两个生效版本。
        // 期间失败数始终为 0（失败率 0 未超 0.5 阈值），任务保持 RUNNING，上报后达标自动完成。
        success(r, "offline-1", "v2");
        String v1 = store.findDevice("online-1").orElseThrow().getCurrentVersion();
        String v2 = store.findDevice("offline-1").orElseThrow().getCurrentVersion();
        Rollout done = store.findRollout("r-reconnect").orElseThrow();
        log.info("[测试输入] 重连设备补报成功；[判定依据] 生效版本只能是任务目标 v2；[最终状态] versions={}/{}, state={}",
                v1, v2, done.getState());
        assertEquals("v2", v1);
        assertEquals("v2", v2);
        assertEquals(RolloutState.COMPLETED, done.getState());
        assertEquals(DeviceStatus.ONLINE, deviceService.status("offline-1"));
    }

    @Test
    @DisplayName("回滚期间离线设备重新上线：重发回滚命令，最终收敛到基线版本而非中间态")
    void offlineDeviceReconnectsDuringRollback() {
        String dev = "rb-offline";
        registerDevices(List.of(dev), "v1");
        Rollout r = rolloutService.create("r-rb-reconnect", "v2", "v1",
                List.of(List.of(dev)), 1.0d, 1.0d, 3);
        rolloutService.start(r.getRolloutId());
        rolloutService.dispatchUpgrade(r.getRolloutId(), null, dev);
        success(r, dev, "v2");

        // 手动回滚（RUNNING 状态下），设备离线导致回滚命令丢失
        rolloutService.rollback(r.getRolloutId(), null);
        clock.advanceMillis(30_000);
        store.removeCommandsForDevice(dev);
        assertEquals(DevicePhase.ROLLING_BACK,
                store.findRollout("r-rb-reconnect").orElseThrow().getDeviceStates().get(dev).getPhase());

        // 重新上线 -> 心跳触发回滚命令重发
        deviceService.heartbeat(dev, null);
        boolean reissued = store.findCommand("cmd-rb-r-rb-reconnect-" + dev).isPresent();
        log.info("[测试输入] 设备升级到v2后回滚，回滚命令在离线期丢失，重新上线心跳；[判定依据] ROLLING_BACK且无在途命令->重发回滚命令(base=v1)；[最终状态] 重发={}",
                reissued);
        assertTrue(reissued);

        // 回滚期间设备的任何结果上报都解释为回滚结果（重复上报幂等），目标版本唯一为 base
        rolloutService.reportResult(r.getRolloutId(), dev, true, "v1", null);
        rolloutService.reportResult(r.getRolloutId(), dev, true, "v1", null);
        Rollout done = store.findRollout("r-rb-reconnect").orElseThrow();
        String version = store.findDevice(dev).orElseThrow().getCurrentVersion();
        log.info("[测试输入] 设备上报结果；[判定依据] ROLLING_BACK 状态下结果一律解释为回滚结果，目标版本=base；[最终状态] state={}, 设备版本={}, phase={}",
                done.getState(), version, done.getDeviceStates().get(dev).getPhase());
        assertEquals(RolloutState.ROLLED_BACK, done.getState());
        assertEquals("v1", version);
        assertEquals(DevicePhase.ROLLED_BACK, done.getDeviceStates().get(dev).getPhase());
    }

    @Test
    @DisplayName("健康任务手动回滚：原因记为 MANUAL_ROLLBACK，全部回到基线版本")
    void manualRollbackOfHealthyRunningRollout() {
        String dev = "healthy";
        registerDevices(List.of(dev), "v1");
        Rollout r = rolloutService.create("r-manual", "v2", "v1",
                List.of(List.of(dev)), 1.0d, 1.0d, 3);
        rolloutService.start(r.getRolloutId());
        rolloutService.dispatchUpgrade(r.getRolloutId(), null, dev);
        success(r, dev, "v2");
        // 单批 100% 成功 -> COMPLETED
        assertEquals(RolloutState.COMPLETED, store.findRollout("r-manual").orElseThrow().getState());

        Rollout rolling = rolloutService.rollback(r.getRolloutId(), null);
        log.info("[测试输入] 已完成任务手动回滚；[判定依据] 无前置失败原因时记 MANUAL_ROLLBACK；[最终状态] state={}, reason={}",
                rolling.getState(), rolling.getFailureReason());
        assertEquals(FailureReason.MANUAL_ROLLBACK, rolling.getFailureReason());

        rolloutService.reportResult(r.getRolloutId(), dev, true, "v1", null);
        Rollout done = store.findRollout("r-manual").orElseThrow();
        log.info("[测试输入] 回滚结果成功；[最终状态] state={}, version={}, phase={}",
                done.getState(), store.findDevice(dev).orElseThrow().getCurrentVersion(),
                done.getDeviceStates().get(dev).getPhase());
        assertEquals(RolloutState.ROLLED_BACK, done.getState());
        assertEquals("v1", store.findDevice(dev).orElseThrow().getCurrentVersion());
    }

    @Test
    @DisplayName("重试上限：maxAttempts=1（仅首次）时不允许重试")
    void retryAttemptLimitEnforced() {
        String dev = "limited";
        registerDevices(List.of(dev), "v1");
        Rollout r = rolloutService.create("r-limit", "v2", "v1",
                List.of(List.of(dev)), 1.0d, 0.0d, 1);
        rolloutService.start(r.getRolloutId());
        rolloutService.dispatchUpgrade(r.getRolloutId(), null, dev);
        failure(r, dev, "fatal");
        assertEquals(RolloutState.PAUSED, store.findRollout("r-limit").orElseThrow().getState());

        boolean blocked = false;
        try {
            rolloutService.retryFailed(r.getRolloutId(), null, dev);
        } catch (Exception e) {
            blocked = true;
            log.info("[测试输入] maxAttempts=1 的设备首次失败后请求重试；[判定依据] usedAttempts+1>=maxAttempts；[最终状态] 被拒绝: {}",
                    e.getMessage());
        }
        assertTrue(blocked, "达到最大尝试次数后必须拒绝重试");
        // 仍然可以回滚
        rolloutService.rollback(r.getRolloutId(), null);
        rolloutService.reportResult(r.getRolloutId(), dev, true, "v1", null);
        assertEquals(RolloutState.ROLLED_BACK, store.findRollout("r-limit").orElseThrow().getState());
    }

    /** 轻量断言辅助，避免测试类里堆太多重复代码。 */
    private static class DeviceTaskPhaseAsserts {
        static void assertUpgrading(Rollout r, String deviceId) {
            assertEquals(DevicePhase.UPGRADING, r.getDeviceStates().get(deviceId).getPhase());
        }
    }
}
