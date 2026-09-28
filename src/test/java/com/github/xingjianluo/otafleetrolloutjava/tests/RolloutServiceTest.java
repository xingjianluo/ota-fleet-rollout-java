package com.github.xingjianluo.otafleetrolloutjava.tests;

import com.github.xingjianluo.otafleetrolloutjava.config.OtaProperties;
import com.github.xingjianluo.otafleetrolloutjava.domain.BatchSuccessMode;
import com.github.xingjianluo.otafleetrolloutjava.domain.DeviceUpgradeRecord;
import com.github.xingjianluo.otafleetrolloutjava.domain.DeviceUpgradeState;
import com.github.xingjianluo.otafleetrolloutjava.domain.TaskStatus;
import com.github.xingjianluo.otafleetrolloutjava.domain.UpgradeTask;
import com.github.xingjianluo.otafleetrolloutjava.service.DeviceRegistry;
import com.github.xingjianluo.otafleetrolloutjava.service.RolloutService;
import com.github.xingjianluo.otafleetrolloutjava.service.RolloutService.ReportOutcome;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 灰度升级编排测试：批次推进边界、重复上报幂等、失败暂停、
 * 重试回滚、离线设备重连收敛、并发上报一致性。
 * 每个用例打印：输入、判定依据、最终状态。
 */
class RolloutServiceTest {

    private static final Logger log = LoggerFactory.getLogger(RolloutServiceTest.class);
    private static final long T0 = 1_000_000L;

    private DeviceRegistry registry;
    private RolloutService rolloutService;

    @BeforeEach
    void setUp() {
        OtaProperties properties = new OtaProperties();
        properties.setOfflineThresholdMs(1_000L);
        registry = new DeviceRegistry(properties);
        rolloutService = new RolloutService(registry, properties);
    }

    private void registerDevices(String prefix, int count) {
        for (int i = 1; i <= count; i++) {
            registry.register(prefix + i, "1.0.0", T0);
        }
    }

    private List<String> deviceIds(String prefix, int count) {
        List<String> ids = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            ids.add(prefix + i);
        }
        return ids;
    }

    @Test
    void batchAdvancesOnlyWhenThresholdReached() {
        registerDevices("d", 4);
        // 2 台一批共 2 批，阈值 2：必须整批成功才能推进
        UpgradeTask task = rolloutService.createTask("t1", "2.0.0", deviceIds("d", 4),
                2, 2, 0.5d);

        rolloutService.reportSuccess("t1", "d1");
        boolean advancedAfterOne = rolloutService.advanceBatch("t1");
        log.info("[用例:批次阈值] 输入: 批大小=2, 阈值=2, 第1批仅1台成功");
        log.info("[用例:批次阈值] 判定依据: successCount(1) < threshold(2) -> 不推进");
        log.info("[用例:批次阈值] 最终状态: advanced={}, currentBatch={}", advancedAfterOne, task.getCurrentBatchIndex());
        assertFalse(advancedAfterOne, "未达阈值不得推进");
        assertEquals(0, task.getCurrentBatchIndex());

        rolloutService.reportSuccess("t1", "d2"); // 达到阈值，settle 自动推进
        log.info("[用例:批次阈值] 输入: 第1批第2台成功");
        log.info("[用例:批次阈值] 判定依据: successCount(2) >= threshold(2) -> 自动推进");
        log.info("[用例:批次阈值] 最终状态: currentBatch={}", task.getCurrentBatchIndex());
        assertEquals(1, task.getCurrentBatchIndex(), "达到阈值后应推进到第2批");

        // 第2批未开始前的上报限制：d3 属于当前批可上报
        assertEquals(ReportOutcome.ACCEPTED_SUCCESS, rolloutService.reportSuccess("t1", "d3"));
        assertEquals(ReportOutcome.ACCEPTED_SUCCESS, rolloutService.reportSuccess("t1", "d4"));
        assertEquals(TaskStatus.COMPLETED, task.getStatus(), "全部批次达标后任务完成");
        log.info("[用例:批次阈值] 最终状态: taskStatus={}", task.getStatus());
    }

    @Test
    void futureBatchReportRejectedToPreventSkip() {
        registerDevices("e", 4);
        rolloutService.createTask("t2", "2.0.0", deviceIds("e", 4), 2, 1, 0.5d);

        // e3 属于第2批，在第1批未达标前上报必须被拒绝（防跳批）
        ReportOutcome outcome = rolloutService.reportSuccess("t2", "e3");
        UpgradeTask task = rolloutService.findTask("t2").orElseThrow();
        log.info("[用例:防跳批] 输入: 当前批次=0, 设备e3属于批次1, 提前上报成功");
        log.info("[用例:防跳批] 判定依据: recordBatch(1) > currentBatch(0) -> 拒绝");
        log.info("[用例:防跳批] 最终状态: outcome={}, currentBatch={}, e3状态={}",
                outcome, task.getCurrentBatchIndex(), task.findRecord("e3").getState());

        assertEquals(ReportOutcome.REJECTED_BATCH_NOT_STARTED, outcome);
        assertEquals(0, task.getCurrentBatchIndex());
        assertEquals(DeviceUpgradeState.PENDING, task.findRecord("e3").getState());
    }

    @Test
    void duplicateReportsDoNotDoubleCount() {
        registerDevices("f", 2);
        UpgradeTask task = rolloutService.createTask("t3", "2.0.0", deviceIds("f", 2), 2, 2, 0.5d);

        assertEquals(ReportOutcome.ACCEPTED_SUCCESS, rolloutService.reportSuccess("t3", "f1"));
        // 重复投递同一成功上报
        assertEquals(ReportOutcome.DUPLICATE_IGNORED, rolloutService.reportSuccess("t3", "f1"));
        assertEquals(ReportOutcome.DUPLICATE_IGNORED, rolloutService.reportSuccess("t3", "f1"));
        // 同一设备先成功后又收到失败上报（乱序重复），不得改判
        assertEquals(ReportOutcome.DUPLICATE_IGNORED, rolloutService.reportFailure("t3", "f1", "late-failure"));

        long successCount = task.currentBatchSuccessCount();
        log.info("[用例:重复上报] 输入: f1成功上报x3 + 迟到失败上报x1");
        log.info("[用例:重复上报] 判定依据: 终态只计数一次, 后续上报幂等忽略");
        log.info("[用例:重复上报] 最终状态: successCount={}, f1状态={}", successCount, task.findRecord("f1").getState());

        assertEquals(1, successCount, "重复上报不得重复计数");
        assertEquals(DeviceUpgradeState.SUCCESS, task.findRecord("f1").getState());
        assertEquals(1, task.findRecord("f1").getAttempts());
    }

    @Test
    void taskPausesWhenFailureRateExceededAndKeepsReason() {
        registerDevices("g", 4);
        // maxFailureRate=0.4：第1个失败即 1/1=1.0 > 0.4，任务暂停
        UpgradeTask task = rolloutService.createTask("t4", "2.0.0", deviceIds("g", 4), 2, 1, 0.4d);

        assertEquals(ReportOutcome.ACCEPTED_FAILURE, rolloutService.reportFailure("t4", "g1", "flash-write-error"));
        log.info("[用例:失败暂停] 输入: g1失败(原因=flash-write-error), maxFailureRate=0.4");
        log.info("[用例:失败暂停] 判定依据: failureRate=1/1=1.0 > 0.4 -> 任务暂停");
        log.info("[用例:失败暂停] 最终状态: status={}, pauseReason={}", task.getStatus(), task.getPauseReason());

        assertEquals(TaskStatus.PAUSED, task.getStatus());
        assertNotNull(task.getPauseReason());
        assertTrue(task.getPauseReason().contains("g1:flash-write-error"), "暂停原因需可追溯");
        assertEquals("flash-write-error", task.findRecord("g1").getFailReason());

        // 暂停期间上报被拒绝
        assertEquals(ReportOutcome.REJECTED_TASK_PAUSED, rolloutService.reportSuccess("t4", "g2"));
    }

    @Test
    void retryFailedDeviceAndResume() {
        registerDevices("h", 2);
        UpgradeTask task = rolloutService.createTask("t5", "2.0.0", deviceIds("h", 2), 2, 1, 0.4d);

        rolloutService.reportFailure("t5", "h1", "network-timeout");
        assertEquals(TaskStatus.PAUSED, task.getStatus());

        assertTrue(rolloutService.retryDevice("t5", "h1"));
        log.info("[用例:重试] 输入: h1失败后重试");
        log.info("[用例:重试] 判定依据: FAILED->PENDING且失败记录清零 -> 任务自动恢复");
        log.info("[用例:重试] 最终状态: h1状态={}, taskStatus={}",
                task.findRecord("h1").getState(), task.getStatus());
        assertEquals(DeviceUpgradeState.PENDING, task.findRecord("h1").getState());
        assertEquals(TaskStatus.RUNNING, task.getStatus(), "失败消解后任务应恢复");

        // 重试后设备重新上报成功
        assertEquals(ReportOutcome.ACCEPTED_SUCCESS, rolloutService.reportSuccess("t5", "h1"));
        assertEquals("2.0.0", registry.find("h1").orElseThrow().getEffectiveVersion());
        assertEquals(2, task.findRecord("h1").getAttempts(), "重试后再次上报计为第2次尝试");
    }

    @Test
    void rollbackRestoresUniqueEffectiveVersion() {
        registerDevices("i", 2);
        UpgradeTask task = rolloutService.createTask("t6", "2.0.0", deviceIds("i", 2), 2, 1, 0.4d);

        rolloutService.reportFailure("t6", "i1", "checksum-mismatch");
        assertEquals(TaskStatus.PAUSED, task.getStatus());

        assertTrue(rolloutService.rollbackDevice("t6", "i1"));
        String version = registry.find("i1").orElseThrow().getEffectiveVersion();
        log.info("[用例:回滚] 输入: i1失败后回滚");
        log.info("[用例:回滚] 判定依据: FAILED->ROLLED_BACK, 生效版本恢复为previousVersion(1.0.0)");
        log.info("[用例:回滚] 最终状态: i1生效版本={}, 记录状态={}, taskStatus={}",
                version, task.findRecord("i1").getState(), task.getStatus());

        assertEquals("1.0.0", version, "回滚后生效版本必须唯一且确定为升级前版本");
        assertEquals(DeviceUpgradeState.ROLLED_BACK, task.findRecord("i1").getState());
        assertEquals(TaskStatus.RUNNING, task.getStatus(), "失败消解后任务恢复");

        // 回滚是终态：重复回滚/上报均幂等
        assertFalse(rolloutService.rollbackDevice("t6", "i1"));
        assertEquals(ReportOutcome.DUPLICATE_IGNORED, rolloutService.reportSuccess("t6", "i1"));
        assertEquals("1.0.0", registry.find("i1").orElseThrow().getEffectiveVersion());
    }

    @Test
    void offlineDeviceConvergesToTargetVersionOnReconnect() {
        registerDevices("j", 4);
        UpgradeTask task = rolloutService.createTask("t7", "2.0.0", deviceIds("j", 4), 2, 2, 0.5d);

        // 第1批：j1成功，j2离线未上报；阈值2未达 -> 不推进
        rolloutService.reportSuccess("t7", "j1");
        assertEquals(0, task.getCurrentBatchIndex());

        // j2 离线后重新上线：心跳触发收敛，重新下发命令
        registry.heartbeat("j2", T0 + 500);
        List<String> commands = rolloutService.convergeOnReconnect("j2");
        log.info("[用例:重连收敛] 输入: j2离线后重连, 当前版本=1.0.0, 任务目标=2.0.0");
        log.info("[用例:重连收敛] 判定依据: 记录PENDING且版本落后 -> 重新下发升级命令");
        log.info("[用例:重连收敛] 最终状态: 下发命令={}", commands);
        assertEquals(List.of("t7->2.0.0"), commands);

        // j2 完成升级，第1批达标推进
        assertEquals(ReportOutcome.ACCEPTED_SUCCESS, rolloutService.reportSuccess("t7", "j2"));
        assertEquals(1, task.getCurrentBatchIndex());
        assertEquals("2.0.0", registry.find("j2").orElseThrow().getEffectiveVersion());

        // 收敛后再次重连：版本已对齐，不再重复下发
        List<String> none = rolloutService.convergeOnReconnect("j2");
        log.info("[用例:重连收敛] 收敛后重连: 下发命令={} (应为空, 版本已对齐)", none);
        assertTrue(none.isEmpty(), "版本对齐后不得重复下发命令");
        assertEquals("2.0.0", registry.find("j2").orElseThrow().getEffectiveVersion(),
                "生效版本唯一且确定, 不存在中间态");
    }

    @Test
    void ratioModeTailBatchCompletesWhenDeviceCountNotMultiple() {
        // 5 台设备，每批 2 台 => 2 个满批 + 1 个只有 1 台的尾巴批
        registerDevices("m", 5);
        UpgradeTask task = rolloutService.createTask("t9", "2.0.0", deviceIds("m", 5),
                2, 2, 0.5d, BatchSuccessMode.RATIO, 1.0d, 0);

        // 前两批各 2 台全部成功，自动逐批推进
        rolloutService.reportSuccess("t9", "m1");
        rolloutService.reportSuccess("t9", "m2");
        rolloutService.reportSuccess("t9", "m3");
        rolloutService.reportSuccess("t9", "m4");
        log.info("[用例:比例收尾] 输入: 5台/每批2台, RATIO模式 ratio=1.0, 前4台成功, 尾巴批仅m5一台");
        log.info("[用例:比例收尾] 判定依据: 尾巴批成功占比=1/1=1.0 >= 1.0 -> 达标(不会因不足固定台数而卡死)");

        assertEquals(2, task.getCurrentBatchIndex(), "前两批完成后应推进到尾巴批(索引2)");

        // 尾巴批唯一一台设备成功：比例达标且全部终态 -> 任务 COMPLETED
        assertEquals(ReportOutcome.ACCEPTED_SUCCESS, rolloutService.reportSuccess("t9", "m5"));
        log.info("[用例:比例收尾] 最终状态: currentBatch={}/{}, taskStatus={}",
                task.getCurrentBatchIndex() + 1, task.getBatchCount(), task.getStatus());
        assertEquals(TaskStatus.COMPLETED, task.getStatus(), "尾巴批按比例达标后整个任务必须能完成");
    }

    @Test
    void ratioModePassesTailBatchByRatioWhileCountThresholdWouldNotBeReachable() {
        // 7 台设备，每批 2 台 => 三个满批 + 1 个只有 1 台的尾巴批。
        // RATIO 模式 0.6：满批 2 台需至少 2 台成功(1.0>=0.6)，尾巴批 1 台成功即 1/1=1.0 达标；
        // 对照：若沿用固定台数阈值 3，尾巴批永远只有 1 台、不可能达标。
        registerDevices("n", 7);
        UpgradeTask task = rolloutService.createTask("t10", "2.0.0", deviceIds("n", 7),
                2, 3, 0.5d, BatchSuccessMode.RATIO, 0.6d, 0);
        assertEquals(4, task.getBatchCount(), "7台/每批2台 应为4批(含1台尾巴批)");

        for (String d : List.of("n1", "n2", "n3", "n4", "n5", "n6", "n7")) {
            assertEquals(ReportOutcome.ACCEPTED_SUCCESS, rolloutService.reportSuccess("t10", d));
        }
        log.info("[用例:比例vs台数] 输入: 7台/每批2台, RATIO=0.6, 全部成功(尾巴批仅n7一台)");
        log.info("[用例:比例vs台数] 判定依据: 各批成功占比 2/2=1.0、1/1=1.0 均 >= 0.6 -> 逐批达标");
        log.info("[用例:比例vs台数] 最终状态: taskStatus={}", task.getStatus());
        assertEquals(TaskStatus.COMPLETED, task.getStatus(), "比例模式下尾巴批正常达标, 任务不卡死");
    }

    @Test
    void abortFromPausedRollsBackFailedDeviceAndReachesTerminalState() {
        registerDevices("w", 2);
        UpgradeTask task = rolloutService.createTask("t17", "2.0.0", deviceIds("w", 2),
                2, 1, 0.4d, BatchSuccessMode.COUNT, 1.0d, 0);
        // w1 失败 -> 失败率 1.0 > 0.4，任务进入 PAUSED
        rolloutService.reportFailure("t17", "w1", "flash-error");
        assertEquals(TaskStatus.PAUSED, task.getStatus());

        // 暂停状态下一键中止：FAILED 的 w1 回滚，PENDING 的 w2 跳过
        boolean aborted = rolloutService.abortTask("t17");
        log.info("[用例:暂停态中止] 输入: 任务因失败率超限PAUSED(w1=FAILED,w2=PENDING), 一键中止");
        log.info("[用例:暂停态中止] 判定依据: PAUSED也可中止 -> FAILED回滚旧版本, PENDING跳过, 任务终态ABORTED");
        assertTrue(aborted);
        assertEquals(TaskStatus.ABORTED, task.getStatus());
        assertEquals(DeviceUpgradeState.ROLLED_BACK, task.findRecord("w1").getState());
        assertEquals(DeviceUpgradeState.SKIPPED, task.findRecord("w2").getState());
        assertEquals("1.0.0", registry.find("w1").orElseThrow().getEffectiveVersion());
        assertEquals("1.0.0", registry.find("w2").orElseThrow().getEffectiveVersion());
        log.info("[用例:暂停态中止] 最终状态: taskStatus={}, w1={}, w2={}, 全设备版本=1.0.0",
                task.getStatus(), task.findRecord("w1").getState(), task.findRecord("w2").getState());
    }

    @Test
    void ratioModeDoesNotAdvanceWhenRatioBelowThreshold() {
        // 满批 3 台，比例 0.7：仅 2 台成功 => 2/3=0.667 < 0.7，不得推进
        registerDevices("v", 6);
        UpgradeTask task = rolloutService.createTask("t16", "2.0.0", deviceIds("v", 6),
                3, 99, 1.0d, BatchSuccessMode.RATIO, 0.7d, 0);
        rolloutService.reportSuccess("t16", "v1");
        rolloutService.reportSuccess("t16", "v2");
        boolean advanced = rolloutService.advanceBatch("t16");
        log.info("[用例:比例边界] 输入: 每批3台, RATIO=0.7, 当前批2台成功");
        log.info("[用例:比例边界] 判定依据: 成功占比=2/3=0.667 < 0.7 -> 不推进");
        assertFalse(advanced, "比例未达标同样不得推进");
        assertEquals(0, task.getCurrentBatchIndex());

        // 第 3 台成功 => 3/3=1.0 >= 0.7，自动推进
        rolloutService.reportSuccess("t16", "v3");
        log.info("[用例:比例边界] 判定依据: 成功占比=3/3=1.0 >= 0.7 -> 推进");
        log.info("[用例:比例边界] 最终状态: currentBatch={}", task.getCurrentBatchIndex());
        assertEquals(1, task.getCurrentBatchIndex());
    }

    @Test
    void countModeKeepsLegacyBehaviorWhenConfigured() {
        // 老配置（COUNT 模式）：尾巴批只有 1 台、固定阈值为 2 时不能靠比例蒙混过关
        registerDevices("p", 5);
        UpgradeTask task = rolloutService.createTask("t11", "2.0.0", deviceIds("p", 5),
                2, 2, 0.5d, BatchSuccessMode.COUNT, 1.0d, 0);
        rolloutService.reportSuccess("t11", "p1");
        rolloutService.reportSuccess("t11", "p2");
        rolloutService.reportSuccess("t11", "p3");
        rolloutService.reportSuccess("t11", "p4");
        assertEquals(2, task.getCurrentBatchIndex());
        rolloutService.reportSuccess("t11", "p5");
        // COUNT 模式下尾巴批 successCount(1) < threshold(2)，advanceBatch 不会判定完成
        boolean advanced = rolloutService.advanceBatch("t11");
        log.info("[用例:台数兼容] 输入: COUNT模式 threshold=2, 尾巴批仅1台成功");
        log.info("[用例:台数兼容] 判定依据: successCount(1) < threshold(2) -> 不按比例放行(老行为不变)");
        log.info("[用例:台数兼容] 最终状态: advanced={}, taskStatus={}", advanced, task.getStatus());
        assertFalse(advanced, "COUNT 模式必须保持按固定台数判定的历史行为");
        assertEquals(TaskStatus.RUNNING, task.getStatus());
    }

    @Test
    void abortRollsBackSuccessfulDevicesAndSkipsPendingOnes() {
        registerDevices("q", 4);
        UpgradeTask task = rolloutService.createTask("t12", "2.0.0", deviceIds("q", 4),
                4, 1, 0.5d, BatchSuccessMode.COUNT, 1.0d, 0);

        // q1、q2 已升级成功（新版本），q3、q4 尚未升级（PENDING，旧版本）
        rolloutService.reportSuccess("t12", "q1");
        rolloutService.reportSuccess("t12", "q2");

        boolean aborted = rolloutService.abortTask("t12");
        log.info("[用例:整任务中止] 输入: q1/q2已成功(2.0.0), q3/q4未升级(1.0.0), 一键中止");
        log.info("[用例:整任务中止] 判定依据: SUCCESS/FAILED -> ROLLED_BACK并恢复旧版本; PENDING -> SKIPPED保持原样");

        assertTrue(aborted);
        assertEquals(TaskStatus.ABORTED, task.getStatus(), "任务到达明确的中止终态");
        assertEquals(DeviceUpgradeState.ROLLED_BACK, task.findRecord("q1").getState());
        assertEquals(DeviceUpgradeState.ROLLED_BACK, task.findRecord("q2").getState());
        assertEquals(DeviceUpgradeState.SKIPPED, task.findRecord("q3").getState());
        assertEquals(DeviceUpgradeState.SKIPPED, task.findRecord("q4").getState());

        // 版本唯一确定：成功设备退回 1.0.0，未升级设备始终是 1.0.0
        for (String d : List.of("q1", "q2", "q3", "q4")) {
            assertEquals("1.0.0", registry.find(d).orElseThrow().getEffectiveVersion(),
                    "中止后所有设备版本必须唯一确定为升级前版本: " + d);
        }
        log.info("[用例:整任务中止] 最终状态: taskStatus={}, q1={}(原因={}), q3={}, 全设备版本=1.0.0",
                task.getStatus(), task.findRecord("q1").getState(),
                task.findRecord("q1").getRollbackReason(), task.findRecord("q3").getState());

        // 中止是终态且幂等：重复中止不再执行，上报被拒绝，重连不再下发升级命令
        assertFalse(rolloutService.abortTask("t12"), "重复中止必须幂等");
        assertEquals(ReportOutcome.REJECTED_TASK_ABORTED, rolloutService.reportSuccess("t12", "q3"));
        assertEquals(DeviceUpgradeState.SKIPPED, task.findRecord("q3").getState());
        assertEquals("1.0.0", registry.find("q3").orElseThrow().getEffectiveVersion());
        assertTrue(rolloutService.convergeOnReconnect("q3").isEmpty(), "已中止任务不再收敛升级");
        assertFalse(rolloutService.rollbackDevice("t12", "q1"), "已回滚设备不可重复回滚");
        assertEquals("1.0.0", registry.find("q1").orElseThrow().getEffectiveVersion(), "不得重复回滚/版本错配");
    }

    @Test
    void abortIsConsistentWithConcurrentReports() throws Exception {
        registerDevices("r", 4);
        // 关键构造：2 台一批共 2 批，但固定台数阈值为 4——任何一批都不可能达标推进，
        // 因此无论上报线程多快，任务都不可能在中止前自行 COMPLETED，
        // 从而保证每次运行都真正演练“上报 vs 中止”的竞争，而不是比较谁先跑完。
        UpgradeTask task = rolloutService.createTask("t13", "2.0.0", deviceIds("r", 4),
                2, 4, 1.0d, BatchSuccessMode.COUNT, 1.0d, 0);

        int reportThreads = 8;
        int rounds = 200;
        ExecutorService pool = Executors.newFixedThreadPool(reportThreads + 1);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();

        // 多台设备并发反复上报成功；同时另一个线程一键中止。二者经任务锁串行
        for (int i = 0; i < reportThreads; i++) {
            final String deviceId = "r" + (i % 4 + 1);
            futures.add(pool.submit(() -> {
                try {
                    start.await();
                    for (int j = 0; j < rounds; j++) {
                        rolloutService.reportSuccess("t13", deviceId);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
        }
        Future<?> abortFuture = pool.submit(() -> {
            try {
                start.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            rolloutService.abortTask("t13");
        });
        start.countDown();
        abortFuture.get(30, TimeUnit.SECONDS);
        for (Future<?> f : futures) {
            f.get(30, TimeUnit.SECONDS);
        }
        pool.shutdown();

        log.info("[用例:中止并发上报] 输入: {}线程并发上报成功(每线程{}轮) 与 一键中止 同时发生", reportThreads, rounds);
        log.info("[用例:中止并发上报] 判定依据: 上报与中止经同一任务锁串行, 每设备要么先成功后被回滚, 要么未成功被SKIPPED");

        // 唯一确定的最终结果：任务一定被中止，全部设备版本回到 1.0.0，无一停在半路。
        // 中止先于某设备的成功上报 -> 该上报被 REJECTED_TASK_ABORTED，设备 SKIPPED、版本不动；
        // 成功上报先于中止 -> 设备到新版本后被中止回滚到旧版本。两种顺序都收敛到同一确定结果。
        assertTrue(rolloutService.abortTask("t13") == false, "中止幂等: 再次中止不再执行");
        assertEquals(TaskStatus.ABORTED, task.getStatus());
        int rolledBackCount = 0;
        int skippedCount = 0;
        for (String d : List.of("r1", "r2", "r3", "r4")) {
            DeviceUpgradeRecord record = task.findRecord(d);
            String version = registry.find(d).orElseThrow().getEffectiveVersion();
            assertEquals("1.0.0", version, "并发收敛后版本必须唯一确定: " + d);
            assertTrue(record.getState() == DeviceUpgradeState.ROLLED_BACK
                            || record.getState() == DeviceUpgradeState.SKIPPED,
                    "不允许停留在 PENDING/SUCCESS/FAILED 中间态: " + d);
            if (record.getState() == DeviceUpgradeState.ROLLED_BACK) {
                rolledBackCount++;
                assertNotNull(record.getRollbackReason(), "回滚设备必须有可追溯原因: " + d);
            } else {
                skippedCount++;
            }
            // 状态与版本必须自洽：ROLLED_BACK/SKIPPED 都对应旧版本，绝不允许版本对不上
            assertEquals("1.0.0", version);
        }
        assertEquals(4, rolledBackCount + skippedCount, "每台设备都必须到达唯一终态");
        log.info("[用例:中止并发上报] 最终状态: taskStatus={}, 回滚={}台, 跳过={}台, 全设备版本=1.0.0, 无中间态",
                task.getStatus(), rolledBackCount, skippedCount);
    }

    @Test
    void failureAtRetryLimitAutoRollsBackAndBlocksFurtherRetry() {
        registerDevices("s", 2);
        // maxFailureRate=1.0 不因失败率暂停；每设备最多尝试 2 次（首次 + 1 次重试）
        UpgradeTask task = rolloutService.createTask("t14", "2.0.0", deviceIds("s", 2),
                2, 1, 1.0d, BatchSuccessMode.COUNT, 1.0d, 2);

        // 第 1 次失败：attempts=1 < 2，停留 FAILED，允许重试
        rolloutService.reportFailure("t14", "s1", "network-timeout");
        assertEquals(DeviceUpgradeState.FAILED, task.findRecord("s1").getState());
        assertEquals(1, task.findRecord("s1").getAttempts());
        assertTrue(rolloutService.retryDevice("t14", "s1"));
        assertEquals(DeviceUpgradeState.PENDING, task.findRecord("s1").getState());

        // 第 2 次失败：attempts=2 达到上限 -> 自动回滚，版本恢复，记录原因
        assertEquals(ReportOutcome.ACCEPTED_FAILURE,
                rolloutService.reportFailure("t14", "s1", "checksum-mismatch"));
        DeviceUpgradeRecord record = task.findRecord("s1");
        log.info("[用例:重试上限] 输入: 最多尝试2次, s1连续两次失败(network-timeout/checksum-mismatch)");
        log.info("[用例:重试上限] 判定依据: attempts(2) >= maxAttempts(2) -> 直接自动回滚, 不再允许重试");

        assertEquals(DeviceUpgradeState.ROLLED_BACK, record.getState());
        assertEquals(2, record.getAttempts(), "任务查询需能看到设备试了几次");
        assertEquals("checksum-mismatch", record.getFailReason(), "最后一次失败原因保留");
        assertNotNull(record.getRollbackReason(), "任务查询需能看到为何变成回滚");
        assertTrue(record.getRollbackReason().contains("retry-limit-exceeded"));
        assertEquals("1.0.0", registry.find("s1").orElseThrow().getEffectiveVersion());
        log.info("[用例:重试上限] 最终状态: state={}, attempts={}, failReason={}, rollbackReason={}, version={}",
                record.getState(), record.getAttempts(), record.getFailReason(),
                record.getRollbackReason(), registry.find("s1").orElseThrow().getEffectiveVersion());

        // 达到上限后重试被拒绝，不会无限重试；上报/回滚均幂等，版本不再变动
        assertFalse(rolloutService.retryDevice("t14", "s1"), "达到上限不得再重试");
        assertEquals(2, task.findRecord("s1").getAttempts());
        assertFalse(rolloutService.rollbackDevice("t14", "s1"), "已自动回滚不可重复回滚");
        assertEquals("1.0.0", registry.find("s1").orElseThrow().getEffectiveVersion());
    }

    @Test
    void unlimitedRetriesWhenMaxAttemptsZero() {
        // 老配置 maxAttemptsPerDevice=0：重试次数不受限（历史行为不变）
        registerDevices("u", 2);
        UpgradeTask task = rolloutService.createTask("t15", "2.0.0", deviceIds("u", 2),
                2, 1, 1.0d, BatchSuccessMode.COUNT, 1.0d, 0);
        for (int i = 0; i < 5; i++) {
            rolloutService.reportFailure("t15", "u1", "err-" + i);
            assertEquals(DeviceUpgradeState.FAILED, task.findRecord("u1").getState(), "第" + (i + 1) + "次失败后");
            assertTrue(rolloutService.retryDevice("t15", "u1"), "不限次数时始终允许重试");
        }
        assertEquals(ReportOutcome.ACCEPTED_SUCCESS, rolloutService.reportSuccess("t15", "u1"));
        log.info("[用例:重试兼容] 输入: maxAttempts=0(老配置), 失败5次后第6次成功");
        log.info("[用例:重试兼容] 判定依据: maxAttempts<=0 不限制重试(历史行为)");
        log.info("[用例:重试兼容] 最终状态: attempts={}, state={}, version={}",
                task.findRecord("u1").getAttempts(), task.findRecord("u1").getState(),
                registry.find("u1").orElseThrow().getEffectiveVersion());
        assertEquals(6, task.findRecord("u1").getAttempts());
        assertEquals(DeviceUpgradeState.SUCCESS, task.findRecord("u1").getState());
        assertEquals("2.0.0", registry.find("u1").orElseThrow().getEffectiveVersion());
    }

    @Test
    void concurrentReportsAreCountedExactlyOnce() throws Exception {
        registerDevices("k", 4);
        UpgradeTask task = rolloutService.createTask("t8", "2.0.0", deviceIds("k", 4), 4, 4, 0.5d);

        int threads = 16;
        int duplicatesPerThread = 50;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            final String deviceId = "k" + (i % 4 + 1); // 每台设备4个线程并发重复上报
            futures.add(pool.submit(() -> {
                try {
                    start.await();
                    for (int j = 0; j < duplicatesPerThread; j++) {
                        if (rolloutService.reportSuccess("t8", deviceId) == ReportOutcome.ACCEPTED_SUCCESS) {
                            accepted.incrementAndGet();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            f.get(30, TimeUnit.SECONDS);
        }
        pool.shutdown();

        long successCount = task.currentBatchSuccessCount();
        log.info("[用例:并发上报] 输入: {}线程对4台设备并发重复上报成功, 每线程{}次", threads, duplicatesPerThread);
        log.info("[用例:并发上报] 判定依据: 任务锁串行化 + 终态幂等 -> 每台设备只计数一次");
        log.info("[用例:并发上报] 最终状态: accepted={}, successCount={}, taskStatus={}",
                accepted.get(), successCount, task.getStatus());

        assertEquals(4, accepted.get(), "全部并发上报中每台设备仅一次被接受");
        assertEquals(4, successCount);
        assertEquals(TaskStatus.COMPLETED, task.getStatus(), "4台全部成功且达标, 任务完成");
    }
}
