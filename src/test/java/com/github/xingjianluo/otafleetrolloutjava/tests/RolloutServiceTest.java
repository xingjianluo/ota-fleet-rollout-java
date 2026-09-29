package com.github.xingjianluo.otafleetrolloutjava.tests;

import com.github.xingjianluo.otafleetrolloutjava.config.OtaProperties;
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
import static org.junit.jupiter.api.Assertions.assertNull;
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

    /**
     * 收尾批次（设备数不是每批台数整数倍）必须能被正常判定：
     * 线上默认配置 batchSize=3 / threshold=2，投放 4 台 -> 末批仅 1 台，
     * 末批整批成功即放行，任务正常 COMPLETED，而不是永远停在 RUNNING。
     */
    @Test
    void tailBatchCompletesWhenFleetSizeNotMultipleOfBatchSize() {
        OtaProperties defaults = new OtaProperties();
        defaults.setOfflineThresholdMs(1_000L);
        defaults.setBatchSize(3);
        defaults.setBatchSuccessThreshold(2);
        defaults.setMaxFailureRate(0.5d);
        DeviceRegistry defaultRegistry = new DeviceRegistry(defaults);
        RolloutService svc = new RolloutService(defaultRegistry, defaults);
        for (String id : deviceIds("m", 4)) {
            defaultRegistry.register(id, "1.0.0", T0);
        }

        // 使用与线上一致的默认配置创建任务（不显式传批次参数）
        UpgradeTask task = svc.createTask("tail-ok", "2.0.0", deviceIds("m", 4));
        log.info("[用例:收尾批次] 输入: 默认配置 batchSize=3, threshold=2, 设备数=4 -> 批次数={}, 末批台数={}",
                task.getBatchCount(), task.getBatchCount() == 2 ? 1 : -1);
        assertEquals(2, task.getBatchCount(), "3+1 切分为两个批次");

        // 第1批 3 台：2 台成功即达到原配置阈值，自动放行
        assertEquals(ReportOutcome.ACCEPTED_SUCCESS, svc.reportSuccess("tail-ok", "m1"));
        assertEquals(ReportOutcome.ACCEPTED_SUCCESS, svc.reportSuccess("tail-ok", "m2"));
        log.info("[用例:收尾批次] 判定依据: 第1批 successCount=2 >= threshold=2 -> 推进到末批");
        assertEquals(1, task.getCurrentBatchIndex());

        // 末批只有 m4 一台：有效阈值降为 min(2,1)=1，整批成功即可收尾
        assertEquals(1, task.currentBatchEffectiveThreshold(),
                "收尾批次有效阈值应降为本批实际台数，永远够得着");
        assertEquals(ReportOutcome.ACCEPTED_SUCCESS, svc.reportSuccess("tail-ok", "m4"));

        log.info("[用例:收尾批次] 判定依据: 末批 successCount=1 >= effectiveThreshold=1 且全部定性 -> COMPLETED");
        log.info("[用例:收尾批次] 最终状态: taskStatus={}, failureReason={}",
                task.getStatus(), task.getFailureReason());
        assertEquals(TaskStatus.COMPLETED, task.getStatus(), "收尾批次达标后任务必须正常完成");
        assertNull(task.getFailureReason());
    }

    /**
     * 一批内设备已全部定性（成功的成功、失败的回滚）但成功台数不足：
     * 失败率未超限（任务未暂停）路径下，回滚后也必须落入 FAILED 终态，
     * 不能停在 RUNNING 无限等待。
     */
    @Test
    void fullyDecidedBatchBelowThresholdFailsTaskWithoutPause() {
        registerDevices("n", 3);
        // 单批3台、阈值2、maxFailureRate=1.0（任何失败率都不暂停）
        UpgradeTask task = rolloutService.createTask("fail-1", "2.0.0", deviceIds("n", 3),
                3, 2, 1.0d);

        assertEquals(ReportOutcome.ACCEPTED_SUCCESS, rolloutService.reportSuccess("fail-1", "n1"));
        assertEquals(ReportOutcome.ACCEPTED_FAILURE,
                rolloutService.reportFailure("fail-1", "n2", "flash-write-error"));
        assertEquals(ReportOutcome.ACCEPTED_FAILURE,
                rolloutService.reportFailure("fail-1", "n3", "power-loss"));
        assertEquals(TaskStatus.RUNNING, task.getStatus(), "失败率未超限, 不暂停");
        log.info("[用例:批内全定性-未暂停] 输入: n1成功, n2/n3失败, 阈值2, maxFailureRate=1.0");

        // 失败设备全部回滚 -> 本批设备已全部定性(SUCCESS/ROLLED_BACK)，但成功仅1台
        int rolledBack = rolloutService.rollbackAllFailed("fail-1");
        log.info("[用例:批内全定性-未暂停] 判定依据: 回滚{}台后无PENDING/FAILED, successCount(1) < threshold(2) -> FAILED",
                rolledBack);
        log.info("[用例:批内全定性-未暂停] 最终状态: taskStatus={}, failureReason={}",
                task.getStatus(), task.getFailureReason());

        assertEquals(2, rolledBack);
        assertEquals(TaskStatus.FAILED, task.getStatus(), "已无待处理设备且成功不足, 必须失败收尾");
        assertNotNull(task.getFailureReason(), "失败原因需可追溯, 查询时一眼能看出已结束");
        assertEquals(DeviceUpgradeState.ROLLED_BACK, task.findRecord("n2").getState());
        assertEquals(DeviceUpgradeState.ROLLED_BACK, task.findRecord("n3").getState());

        // 终态稳定：显式推进/重复回滚/补报都不得让状态来回跳
        assertFalse(rolloutService.advanceBatch("fail-1"));
        assertFalse(rolloutService.rollbackDevice("fail-1", "n2"));
        assertEquals(ReportOutcome.DUPLICATE_IGNORED, rolloutService.reportSuccess("fail-1", "n2"));
        assertEquals(TaskStatus.FAILED, task.getStatus());
    }

    /**
     * 同一场景走默认失败率(0.5)路径：2/3 失败先触发暂停，全部回滚后任务自动恢复，
     * 恢复瞬间重新收尾 -> FAILED 终态（修复前会停在 RUNNING 永不结束）。
     */
    @Test
    void fullyDecidedBatchBelowThresholdFailsTaskAfterPauseAndRollback() {
        registerDevices("p", 3);
        UpgradeTask task = rolloutService.createTask("fail-2", "2.0.0", deviceIds("p", 3),
                3, 2, 0.5d);

        rolloutService.reportSuccess("fail-2", "p1");
        assertEquals(ReportOutcome.ACCEPTED_FAILURE,
                rolloutService.reportFailure("fail-2", "p2", "checksum-mismatch"));
        // 1/2 = 0.5 未超过 0.5，仍在进行
        assertEquals(TaskStatus.RUNNING, task.getStatus());
        // 第2台失败：2/3 ≈ 0.67 > 0.5 -> 暂停
        assertEquals(ReportOutcome.ACCEPTED_FAILURE,
                rolloutService.reportFailure("fail-2", "p3", "network-timeout"));
        assertEquals(TaskStatus.PAUSED, task.getStatus());
        log.info("[用例:批内全定性-暂停恢复] 输入: p1成功, p2/p3失败 -> failureRate=2/3>0.5 暂停");

        int rolledBack = rolloutService.rollbackAllFailed("fail-2");
        log.info("[用例:批内全定性-暂停恢复] 判定依据: 回滚{}台失败消解->恢复RUNNING, 随即重判: 全部定性但成功不足 -> FAILED",
                rolledBack);
        log.info("[用例:批内全定性-暂停恢复] 最终状态: taskStatus={}, failureReason={}",
                task.getStatus(), task.getFailureReason());

        assertEquals(2, rolledBack);
        assertEquals(TaskStatus.FAILED, task.getStatus(), "恢复后批次已无救，必须明确失败收尾");
        assertNotNull(task.getFailureReason());
    }

    /**
     * 收尾批次并发上报 + 并发显式推进：末批只有一台设备，多线程重复上报成功，
     * 放行与完成只能发生一次，最终状态唯一确定（COMPLETED），不随后续调用来回跳。
     */
    @Test
    void tailBatchConcurrentReportsAndAdvanceSettleExactlyOnce() throws Exception {
        registerDevices("q", 4);
        UpgradeTask task = rolloutService.createTask("tail-conc", "2.0.0", deviceIds("q", 4),
                3, 2, 0.5d);
        // 先让第1批达标放行（2台成功，阈值2）
        rolloutService.reportSuccess("tail-conc", "q1");
        rolloutService.reportSuccess("tail-conc", "q2");
        assertEquals(1, task.getCurrentBatchIndex());

        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            final boolean reporter = i % 2 == 0;
            futures.add(pool.submit(() -> {
                try {
                    start.await();
                    for (int j = 0; j < 50; j++) {
                        if (reporter) {
                            if (rolloutService.reportSuccess("tail-conc", "q4")
                                    == ReportOutcome.ACCEPTED_SUCCESS) {
                                accepted.incrementAndGet();
                            }
                        } else {
                            rolloutService.advanceBatch("tail-conc");
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

        log.info("[用例:收尾批次并发] 判定依据: 任务锁串行化 -> 末批唯一一台设备只接受一次, 收尾只发生一次");
        log.info("[用例:收尾批次并发] 最终状态: accepted={}, taskStatus={}", accepted.get(), task.getStatus());
        assertEquals(1, accepted.get(), "末批设备的成功上报只能被接受一次");
        assertEquals(TaskStatus.COMPLETED, task.getStatus(), "并发推进/上报下任务仍唯一确定地完成");

        // 并发结束后继续补报/推进，终态不得翻转
        assertEquals(ReportOutcome.DUPLICATE_IGNORED, rolloutService.reportSuccess("tail-conc", "q4"));
        assertFalse(rolloutService.advanceBatch("tail-conc"));
        assertEquals(TaskStatus.COMPLETED, task.getStatus());
    }
}
