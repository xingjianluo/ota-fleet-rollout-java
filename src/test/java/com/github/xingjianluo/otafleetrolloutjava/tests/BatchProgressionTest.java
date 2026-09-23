package com.github.xingjianluo.otafleetrolloutjava.tests;

import com.github.xingjianluo.otafleetrolloutjava.config.OtaProperties;
import com.github.xingjianluo.otafleetrolloutjava.model.BatchProgress;
import com.github.xingjianluo.otafleetrolloutjava.model.DevicePhase;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 批次推进边界：成功阈值达标才进入下一批；重复投递/上报不重复计数不跳批；
 * 失败率超限暂停并保留原因。
 */
class BatchProgressionTest {

    private static final Logger log = LoggerFactory.getLogger(BatchProgressionTest.class);

    private MutableClock clock;
    private InMemoryStore store;
    private RolloutService rolloutService;
    private DeviceService deviceService;

    @BeforeEach
    void setUp() {
        clock = new MutableClock();
        store = new InMemoryStore();
        rolloutService = new RolloutService(store, clock);
        OtaProperties props = new OtaProperties();
        deviceService = new DeviceService(store, clock, props, rolloutService);
    }

    private void registerDevices(List<String> ids, String version) {
        ids.forEach(id -> deviceService.register(id, version));
    }

    private void sendUpgrade(Rollout r, List<String> devices) {
        for (String d : devices) {
            rolloutService.dispatchUpgrade(r.getRolloutId(), null, d);
        }
    }

    private void reportSuccess(Rollout r, String device, String version) {
        rolloutService.reportResult(r.getRolloutId(), device, true, version, null);
    }

    private void reportFailure(Rollout r, String device, String error) {
        rolloutService.reportResult(r.getRolloutId(), device, false, null, error);
    }

    @Test
    @DisplayName("成功阈值边界：批次 4 台、阈值 0.75，3 成功 1 失败恰好在批次结束时推进下一批")
    void successThresholdBoundaryAdvancesBatch() {
        List<String> b0 = List.of("d0", "d1", "d2", "d3");
        List<String> b1 = List.of("d4", "d5");
        registerDevices(b0, "v1");
        registerDevices(b1, "v1");
        Rollout r = rolloutService.create("r-boundary", "v2", "v1",
                List.of(b0, b1), 0.75d, 0.5d, 3);
        rolloutService.start(r.getRolloutId());
        sendUpgrade(r, b0);

        reportSuccess(r, "d0", "v2");
        reportSuccess(r, "d1", "v2");
        reportFailure(r, "d2", "download-failed");
        Rollout mid = store.findRollout("r-boundary").orElseThrow();
        log.info("[测试输入] 4 台批次已上报 成功x2 失败x1，阈值 success=0.75 failure=0.5；"
                        + "[判定依据] 还有 1 台未决，失败率 1/4=0.25 未超阈值，不推进也不暂停；[最终状态] state={}, currentBatch={}",
                mid.getState(), mid.getCurrentBatch());
        assertEquals(RolloutState.RUNNING, mid.getState());
        assertEquals(0, mid.getCurrentBatch());

        reportSuccess(r, "d3", "v2");
        Rollout after = store.findRollout("r-boundary").orElseThrow();
        BatchProgress p = after.getBatchProgress().get(0);
        log.info("[测试输入] 最后一台上报成功，3/4=0.75 达标；[判定依据] successRate>=阈值 -> 打开下一批；"
                        + "[最终状态] state={}, currentBatch={}, batch0(s={},f={})",
                after.getState(), after.getCurrentBatch(), p.getSucceeded(), p.getFailed());
        assertEquals(RolloutState.RUNNING, after.getState());
        assertEquals(1, after.getCurrentBatch(), "达标后必须推进到第 1 批");
        assertEquals(3, p.getSucceeded());
        assertEquals(1, p.getFailed());
    }

    @Test
    @DisplayName("重复命令投递与重复结果上报幂等：计数不变，不跳批")
    void duplicateDispatchAndReportAreIdempotent() {
        List<String> b0 = List.of("d0", "d1");
        registerDevices(b0, "v1");
        Rollout r = rolloutService.create("r-dup", "v2", "v1",
                List.of(b0, List.of("d2")), 1.0d, 0.0d, 3);
        rolloutService.start(r.getRolloutId());

        // 同一设备重复投递升级命令（显式相同 commandId + 确定性重投）
        rolloutService.dispatchUpgrade(r.getRolloutId(), "cmd-1", "d0");
        rolloutService.dispatchUpgrade(r.getRolloutId(), "cmd-1", "d0");
        rolloutService.dispatchUpgrade(r.getRolloutId(), null, "d0");
        rolloutService.dispatchUpgrade(r.getRolloutId(), "cmd-2", "d1");

        reportSuccess(r, "d0", "v2");
        // 重复上报 3 次
        reportSuccess(r, "d0", "v2");
        reportSuccess(r, "d0", "v2");
        reportSuccess(r, "d0", "v2");
        reportSuccess(r, "d1", "v2");

        Rollout after = store.findRollout("r-dup").orElseThrow();
        BatchProgress p = after.getBatchProgress().get(0);
        log.info("[测试输入] d0 命令重复投递3次、成功重复上报4次；[判定依据] commandId/去重键幂等；"
                        + "[最终状态] batch0 succeeded={} (期望2), state={}, currentBatch={}",
                p.getSucceeded(), after.getState(), after.getCurrentBatch());
        assertEquals(2, p.getSucceeded(), "重复上报只能计一次");
        assertEquals(0, p.getFailed());
        // 两批结构：第 0 批 2 台全成功(100%>=1.0) -> 进入第 1 批，但不会跳到完成
        assertEquals(1, after.getCurrentBatch());
        assertEquals(RolloutState.RUNNING, after.getState());
    }

    @Test
    @DisplayName("失败率超限：未等批次结束立即暂停，保留 FAILURE_RATE_EXCEEDED 与明细原因")
    void failureRateExceededPausesWithReason() {
        List<String> b0 = List.of("d0", "d1", "d2", "d3");
        registerDevices(b0, "v1");
        Rollout r = rolloutService.create("r-fail", "v2", "v1",
                List.of(b0), 0.9d, 0.2d, 3);
        rolloutService.start(r.getRolloutId());
        sendUpgrade(r, b0);

        reportSuccess(r, "d0", "v2");
        reportFailure(r, "d1", "checksum-error");
        Rollout still = store.findRollout("r-fail").orElseThrow();
        log.info("[测试输入] 成功1 失败1，失败率 1/4=0.25>0.2；[判定依据] 失败率超阈值立即暂停；[最终状态] state={}",
                still.getState());
        assertEquals(RolloutState.PAUSED, still.getState());
        assertEquals(FailureReason.FAILURE_RATE_EXCEEDED, still.getFailureReason());
        assertTrue(still.getFailureDetail().contains("批次 0"), "明细必须可追溯到批次: " + still.getFailureDetail());
        log.info("[可追溯原因] reason={}, detail={}", still.getFailureReason(), still.getFailureDetail());

        // 暂停后迟到的结果不再改变状态（不能自行恢复或推进）
        reportSuccess(r, "d2", "v2");
        Rollout afterLate = store.findRollout("r-fail").orElseThrow();
        log.info("[测试输入] 暂停后 d2 迟到成功上报；[判定依据] 暂停状态不自动解除；[最终状态] state={}, currentBatch={}",
                afterLate.getState(), afterLate.getCurrentBatch());
        assertEquals(RolloutState.PAUSED, afterLate.getState());
        assertEquals(0, afterLate.getCurrentBatch());
    }

    @Test
    @DisplayName("并发结果上报：同设备重复并发上报只计一次，成功数确定且不会跳批")
    void concurrentDuplicateReportsCountOnce() throws Exception {
        int n = 10;
        java.util.List<String> devices = new java.util.ArrayList<>();
        for (int i = 0; i < n; i++) {
            devices.add("d" + i);
        }
        registerDevices(devices, "v1");
        Rollout r = rolloutService.create("r-conc", "v2", "v1",
                List.of(devices), 1.0d, 1.0d, 3);
        rolloutService.start(r.getRolloutId());
        sendUpgrade(r, devices);

        int threadsPerDevice = 8;
        ExecutorService pool = Executors.newFixedThreadPool(16);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(n * threadsPerDevice);
        AtomicReference<Throwable> error = new AtomicReference<>();
        for (String d : devices) {
            for (int t = 0; t < threadsPerDevice; t++) {
                pool.submit(() -> {
                    try {
                        start.await();
                        rolloutService.reportResult(r.getRolloutId(), d, true, "v2", null);
                    } catch (Throwable e) {
                        error.compareAndSet(null, e);
                    } finally {
                        done.countDown();
                    }
                });
            }
        }
        start.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS));
        pool.shutdown();
        if (error.get() != null) {
            throw new AssertionError("并发上报出现异常", error.get());
        }
        Rollout after = store.findRollout("r-conc").orElseThrow();
        BatchProgress p = after.getBatchProgress().get(0);
        log.info("[测试输入] 10 台设备 x 8 线程并发重复成功上报(共80次)；[判定依据] 任务锁串行+去重键；"
                        + "[最终状态] succeeded={}(期望10), failed={}, state={}",
                p.getSucceeded(), p.getFailed(), after.getState());
        assertEquals(10, p.getSucceeded());
        assertEquals(0, p.getFailed());
        assertEquals(RolloutState.COMPLETED, after.getState(), "唯一批次 100% 成功后应完成而不是重复推进");
        devices.forEach(d -> assertEquals(DevicePhase.SUCCESS,
                after.getDeviceStates().get(d).getPhase()));
    }
}
