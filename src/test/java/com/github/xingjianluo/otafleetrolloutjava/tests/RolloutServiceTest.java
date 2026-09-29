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
     * 卡死场景一（设备数不是每批整数倍的收尾批次）：
     * 默认配置 batchSize=3、successThreshold=2，投 4 台设备 -> 批次为 [d1,d2,d3] 与收尾批 [d4]。
     * 收尾批只有 1 台，永远凑不满原阈值 2；修复前任务永久停在 RUNNING，
     * 修复后门槛按本批实际台数收敛为 1，d4 成功即 COMPLETED。
     */
    @Test
    void tailBatchSmallerThanThresholdConvergesAndCompletes() {
        // 对齐 application.properties 的生产默认值：batch-size=3, batch-success-threshold=2
        OtaProperties defaults = new OtaProperties();
        defaults.setOfflineThresholdMs(1_000L);
        defaults.setBatchSize(3);
        defaults.setBatchSuccessThreshold(2);
        DeviceRegistry registryWithDefaults = new DeviceRegistry(defaults);
        RolloutService service = new RolloutService(registryWithDefaults, defaults);

        registerDevicesInto("d", 4, registryWithDefaults);
        // 使用配置默认值创建任务（3 台/批, 阈值 2），4 台 -> 2 批，收尾批仅 d4
        UpgradeTask task = service.createTask("t-tail", "2.0.0", deviceIds("d", 4));
        assertEquals(2, task.getBatchCount(), "4台按每批3台切分为2批");
        assertEquals(0, task.getCurrentBatchIndex(), "初始当前批为第0批");

        // 第1批 d1、d2 成功 -> 达到门槛 2，自动推进到收尾批（d3 尚未上报，沿用老规则不等待）
        assertEquals(ReportOutcome.ACCEPTED_SUCCESS, service.reportSuccess("t-tail", "d1"));
        assertEquals(ReportOutcome.ACCEPTED_SUCCESS, service.reportSuccess("t-tail", "d2"));
        assertEquals(1, task.getCurrentBatchIndex(), "第1批达标后应进入收尾批");
        assertEquals(TaskStatus.RUNNING, task.getStatus(), "收尾批未定性前任务保持进行中");

        // 修复前：收尾批只有 d4 一台，成功数永远 < 阈值2，任务卡死在 RUNNING
        ReportOutcome outcome = service.reportSuccess("t-tail", "d4");
        log.info("[用例:收尾批次] 输入: 默认配置(批3台/阈值2)投4台, 收尾批仅d4, d4上报成功");
        log.info("[用例:收尾批次] 判定依据: 收尾批实际台数1 -> 生效门槛min(2,1)=1, successCount(1)>=1 且全部定性 -> 完成");
        log.info("[用例:收尾批次] 最终状态: outcome={}, taskStatus={}, finishReason={}",
                outcome, task.getStatus(), task.getFinishReason());

        assertEquals(ReportOutcome.ACCEPTED_SUCCESS, outcome);
        assertEquals(TaskStatus.COMPLETED, task.getStatus(), "收尾批次按实际台数收敛门槛后必须能正常完成");
        assertNotNull(task.getFinishReason(), "完成收尾需记录可查询的收尾依据");
        // 终态后迟到上报被拒绝，终态不反复
        assertEquals(ReportOutcome.REJECTED_TASK_FINISHED, service.reportSuccess("t-tail", "d4"));
        assertEquals(TaskStatus.COMPLETED, task.getStatus());
    }

    /**
     * 卡死场景二（一批设备全部定性但成功台数不够）：
     * 3 台一批、阈值 2；d1 成功，d2、d3 失败后被回滚。
     * 此时批内已无 PENDING/FAILED 设备（1 SUCCESS + 2 ROLLED_BACK），
     * 成功台数 1 < 2 且不可能再变化。修复前任务永久停在 RUNNING；
     * 修复后明确收尾为 FAILED 终态，finishReason 可追溯。
     */
    @Test
    void batchFailsWhenAllDevicesDecidedButSuccessBelowThreshold() {
        registerDevices("m", 3);
        // 默认失败率 0.5：2 台失败时失败率 2/3≈0.67 > 0.5 -> 会先暂停；
        // 回滚全部失败设备后恢复，此时批内全部定性而成功不足 -> 必须失败收尾
        UpgradeTask task = rolloutService.createTask("t-exhausted", "2.0.0", deviceIds("m", 3),
                3, 2, 0.5d);

        assertEquals(ReportOutcome.ACCEPTED_SUCCESS, rolloutService.reportSuccess("t-exhausted", "m1"));
        assertEquals(ReportOutcome.ACCEPTED_FAILURE, rolloutService.reportFailure("t-exhausted", "m2", "flash-error"));
        // 1/2 失败率 = 0.5，未超过 0.5，仍运行
        assertEquals(TaskStatus.RUNNING, task.getStatus());
        assertEquals(ReportOutcome.ACCEPTED_FAILURE, rolloutService.reportFailure("t-exhausted", "m3", "power-loss"));
        assertEquals(TaskStatus.PAUSED, task.getStatus(), "2/3 失败率超过 0.5, 任务暂停");

        // 两台失败设备全部回滚 -> 失败记录消解, 任务恢复并立即重新收尾判定
        int rolledBack = rolloutService.rollbackAllFailed("t-exhausted");
        log.info("[用例:批内定性失败] 输入: 3台/批阈值2, m1成功, m2/m3失败后全部回滚");
        log.info("[用例:批内定性失败] 判定依据: 批内全部定性(SUCCESS/ROLLED_BACK), successCount(1) < threshold(2), 不可能再变化 -> FAILED");
        log.info("[用例:批内定性失败] 最终状态: rolledBack={}, taskStatus={}, finishReason={}",
                rolledBack, task.getStatus(), task.getFinishReason());

        assertEquals(2, rolledBack);
        assertEquals(DeviceUpgradeState.ROLLED_BACK, task.findRecord("m2").getState());
        assertEquals(DeviceUpgradeState.ROLLED_BACK, task.findRecord("m3").getState());
        assertEquals(TaskStatus.FAILED, task.getStatus(),
                "批内设备已全部定性但成功台数不足时, 任务必须落到明确的失败终态, 不得永久 RUNNING");
        assertNotNull(task.getFinishReason(), "失败收尾原因需可追溯");
        assertTrue(task.getFinishReason().contains("successCount=1"), "收尾原因需体现实际成功台数");

        // 终态确定且不可逆：显式推进无效、迟到上报被拒绝、状态不再来回跳
        assertFalse(rolloutService.advanceBatch("t-exhausted"));
        assertEquals(ReportOutcome.REJECTED_TASK_FINISHED,
                rolloutService.reportSuccess("t-exhausted", "m2"));
        assertEquals(TaskStatus.FAILED, task.getStatus());
    }

    /**
     * 收尾批次的并发确定性：收尾批唯一设备 d4 被多线程并发重复上报成功，
     * 只能被接受一次，任务恰好完成一次，终态唯一确定，不出现反复。
     */
    @Test
    void concurrentReportsOnTailBatchCompleteExactlyOnce() throws Exception {
        registerDevices("n", 4);
        UpgradeTask task = rolloutService.createTask("t-tail-concurrent", "2.0.0", deviceIds("n", 4),
                3, 2, 0.5d);
        rolloutService.reportSuccess("t-tail-concurrent", "n1");
        rolloutService.reportSuccess("t-tail-concurrent", "n2");
        assertEquals(1, task.getCurrentBatchIndex());

        int threads = 12;
        int repeats = 50;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger rejectedFinished = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                try {
                    start.await();
                    for (int j = 0; j < repeats; j++) {
                        ReportOutcome outcome = rolloutService.reportSuccess("t-tail-concurrent", "n4");
                        if (outcome == ReportOutcome.ACCEPTED_SUCCESS) {
                            accepted.incrementAndGet();
                        } else if (outcome == ReportOutcome.REJECTED_TASK_FINISHED
                                || outcome == ReportOutcome.DUPLICATE_IGNORED) {
                            rejectedFinished.incrementAndGet();
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

        log.info("[用例:收尾批并发] 输入: {}线程并发重复上报收尾批唯一设备n4, 每线程{}次", threads, repeats);
        log.info("[用例:收尾批并发] 判定依据: 任务锁串行化 + 终态幂等 -> n4只接受一次, 任务完成一次");
        log.info("[用例:收尾批并发] 最终状态: accepted={}, 其余忽略/拒绝={}, taskStatus={}",
                accepted.get(), rejectedFinished.get(), task.getStatus());

        assertEquals(1, accepted.get(), "收尾批设备的并发重复上报只能接受一次");
        assertEquals(TaskStatus.COMPLETED, task.getStatus(), "收尾批在并发上报下仍唯一确定地完成");
    }

    /**
     * 满批 + 未失败时沿用原放行语义：批大小等于阈值的老配置任务，
     * 必须整批全部成功才完成（门槛未被收尾批规则放宽）。
     */
    @Test
    void fullBatchKeepsOriginalGateSemantics() {
        registerDevices("p", 3);
        UpgradeTask task = rolloutService.createTask("t-full", "2.0.0", deviceIds("p", 3),
                3, 3, 1.0d);

        rolloutService.reportSuccess("t-full", "p1");
        rolloutService.reportSuccess("t-full", "p2");
        assertEquals(TaskStatus.RUNNING, task.getStatus(), "满批阈值3, 仅2台成功不得完成");
        assertFalse(rolloutService.advanceBatch("t-full"), "显式推进同样不得放行");
        assertEquals(3, task.effectiveSuccessThreshold(), "满批生效门槛等于原阈值");

        rolloutService.reportSuccess("t-full", "p3");
        log.info("[用例:满批老语义] 输入: 3台/批阈值3, 三台全部成功");
        log.info("[用例:满批老语义] 判定依据: 满批门槛仍为3, successCount(3)>=3 且全部定性 -> 完成");
        log.info("[用例:满批老语义] 最终状态: taskStatus={}", task.getStatus());
        assertEquals(TaskStatus.COMPLETED, task.getStatus(), "满批老配置的完成语义保持不变");
    }

    /**
     * 变体：失败率未超限（任务未暂停）时直接回滚失败设备，批内同样会全部定性。
     * 3 台/批、阈值 3、maxFailureRate=1.0：m1 成功，m2、m3 失败后回滚，
     * 任务从未 PAUSED；修复前回滚不触发收尾，任务卡在 RUNNING。
     */
    @Test
    void rollbackWhileRunningAlsoFinalizesWhenBatchExhausted() {
        registerDevices("q", 3);
        UpgradeTask task = rolloutService.createTask("t-rollback-running", "2.0.0", deviceIds("q", 3),
                3, 3, 1.0d);

        rolloutService.reportSuccess("t-rollback-running", "q1");
        rolloutService.reportFailure("t-rollback-running", "q2", "flash-error");
        rolloutService.reportFailure("t-rollback-running", "q3", "power-loss");
        assertEquals(TaskStatus.RUNNING, task.getStatus(), "maxFailureRate=1.0, 失败率不超限, 不暂停");

        assertTrue(rolloutService.rollbackDevice("t-rollback-running", "q2"));
        assertEquals(TaskStatus.RUNNING, task.getStatus(), "尚有q3为FAILED未决, 任务继续等待");
        assertTrue(rolloutService.rollbackDevice("t-rollback-running", "q3"));

        log.info("[用例:运行中回滚收尾] 输入: 未暂停, q1成功, q2/q3先后回滚");
        log.info("[用例:运行中回滚收尾] 判定依据: q3回滚后批内全部定性(1 SUCCESS+2 ROLLED_BACK), successCount(1)<3 -> FAILED");
        log.info("[用例:运行中回滚收尾] 最终状态: taskStatus={}, finishReason={}",
                task.getStatus(), task.getFinishReason());

        assertEquals(TaskStatus.FAILED, task.getStatus(),
                "未暂停路径下回滚导致批内全部定性且成功不足时, 也必须明确失败收尾");
    }

    private void registerDevicesInto(String prefix, int count, DeviceRegistry targetRegistry) {
        for (int i = 1; i <= count; i++) {
            targetRegistry.register(prefix + i, "1.0.0", T0);
        }
    }
}
