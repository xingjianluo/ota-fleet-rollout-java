package com.github.xingjianluo.otafleetrolloutjava.service;

import com.github.xingjianluo.otafleetrolloutjava.config.OtaProperties;
import com.github.xingjianluo.otafleetrolloutjava.domain.BatchSuccessMode;
import com.github.xingjianluo.otafleetrolloutjava.domain.Device;
import com.github.xingjianluo.otafleetrolloutjava.domain.DeviceUpgradeRecord;
import com.github.xingjianluo.otafleetrolloutjava.domain.DeviceUpgradeState;
import com.github.xingjianluo.otafleetrolloutjava.domain.TaskStatus;
import com.github.xingjianluo.otafleetrolloutjava.domain.UpgradeTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 灰度升级编排（本地内存模拟）。
 *
 * <p>一致性设计：同一任务的所有变更（上报、推进、重试、回滚）都发生在
 * {@link ConcurrentHashMap#compute} 按 taskId 持有的锁内，因此并发入口
 * （设备上报线程、控制台批推线程、重连收敛线程）看到的状态迁移是串行且可解释的。</p>
 *
 * <p>幂等设计：设备记录的状态机只接受 PENDING -> 终态 的一次性迁移，
 * 命令重复投递或设备重复上报会被识别为重复并忽略，不会重复计数或跳批。</p>
 */
@Service
public class RolloutService {

    private static final Logger log = LoggerFactory.getLogger(RolloutService.class);

    /** 上报结果，用于调用方与测试解释每次上报的处置依据。 */
    public enum ReportOutcome {
        /** 成功上报被接受并计数。 */
        ACCEPTED_SUCCESS,
        /** 失败上报被接受并计数。 */
        ACCEPTED_FAILURE,
        /** 重复上报（记录已处终态），忽略不重复计数。 */
        DUPLICATE_IGNORED,
        /** 设备所在批次尚未开始（拒绝，防止跳批）。 */
        REJECTED_BATCH_NOT_STARTED,
        /** 任务已暂停，需先处理失败设备。 */
        REJECTED_TASK_PAUSED,
        /** 任务已中止/正在中止回滚，上报不再接受。 */
        REJECTED_TASK_ABORTED,
        /** 任务或设备记录不存在。 */
        REJECTED_UNKNOWN
    }

    private final Map<String, UpgradeTask> tasks = new ConcurrentHashMap<>();
    private final DeviceRegistry deviceRegistry;
    private final OtaProperties properties;

    public RolloutService(DeviceRegistry deviceRegistry, OtaProperties properties) {
        this.deviceRegistry = deviceRegistry;
        this.properties = properties;
    }

    /**
     * 创建升级任务（幂等：taskId 已存在时返回既有任务，重复创建命令安全）。
     * 设备按 batchSize 顺序切分为若干批次。
     */
    /**
     * 固定台数判定的便捷重载（历史签名，行为不变：COUNT 模式、不限制重试次数）。
     */
    public UpgradeTask createTask(String taskId, String targetVersion, List<String> deviceIds,
                                  int batchSize, int successThreshold, double maxFailureRate) {
        return createTask(taskId, targetVersion, deviceIds, batchSize, successThreshold, maxFailureRate,
                BatchSuccessMode.COUNT, 1.0d, 0);
    }

    /** 使用配置默认值创建任务。 */
    public UpgradeTask createTask(String taskId, String targetVersion, List<String> deviceIds) {
        return createTask(taskId, targetVersion, deviceIds,
                properties.getBatchSize(),
                properties.getBatchSuccessThreshold(),
                properties.getMaxFailureRate(),
                BatchSuccessMode.valueOf(properties.getBatchSuccessMode()),
                properties.getBatchSuccessRatio(),
                properties.getMaxAttemptsPerDevice());
    }

    /**
     * 全参创建任务。设备按 batchSize 顺序切批；批次达标方式由 batchSuccessMode 决定。
     */
    public UpgradeTask createTask(String taskId, String targetVersion, List<String> deviceIds,
                                  int batchSize, int successThreshold, double maxFailureRate,
                                  BatchSuccessMode batchSuccessMode,
                                  double batchSuccessRatio, int maxAttemptsPerDevice) {
        return tasks.compute(taskId, (id, existing) -> {
            if (existing != null) {
                log.info("[createTask] 重复创建被忽略: taskId={}, 保持既有任务 {}", id, existing);
                return existing;
            }
            UpgradeTask task = new UpgradeTask(id, targetVersion, successThreshold, maxFailureRate,
                    batchSuccessMode, batchSuccessRatio, maxAttemptsPerDevice);
            List<DeviceUpgradeRecord> batch = new ArrayList<>();
            int batchIndex = 0;
            for (String deviceId : deviceIds) {
                String previousVersion = deviceRegistry.find(deviceId)
                        .map(Device::getEffectiveVersion)
                        .orElse("unknown");
                batch.add(new DeviceUpgradeRecord(deviceId, id, batchIndex, targetVersion, previousVersion));
                if (batch.size() == batchSize) {
                    task.addBatch(batch);
                    batch = new ArrayList<>();
                    batchIndex++;
                }
            }
            if (!batch.isEmpty()) {
                task.addBatch(batch);
            }
            log.info("[createTask] 任务创建: taskId={}, targetVersion={}, 设备数={}, 批次数={}, "
                            + "successThreshold={}, maxFailureRate={}, batchSuccessMode={}, batchSuccessRatio={}, maxAttemptsPerDevice={}",
                    id, targetVersion, deviceIds.size(), task.getBatchCount(),
                    successThreshold, maxFailureRate, batchSuccessMode, batchSuccessRatio, maxAttemptsPerDevice);
            return task;
        });
    }

    /**
     * 设备上报升级结果。
     *
     * <p>判定依据（按序）：</p>
     * <ol>
     *   <li>任务/记录不存在 -> REJECTED_UNKNOWN；</li>
     *   <li>任务已中止/正在中止回滚 -> REJECTED_TASK_ABORTED（中止期间上报与回滚串行，结果唯一）；</li>
     *   <li>任务已暂停 -> REJECTED_TASK_PAUSED（先重试或回滚失败设备）；</li>
     *   <li>记录已终态 -> DUPLICATE_IGNORED（不重复计数）；</li>
     *   <li>记录所在批次晚于当前批 -> REJECTED_BATCH_NOT_STARTED（防跳批）；</li>
     *   <li>否则接受：成功则推进设备生效版本到目标版本，失败则记录原因，
     *       且当失败已达该设备尝试次数上限时直接自动回滚；
     *       随后检查失败率（超限暂停）与批次阈值（达标自动推进）。</li>
     * </ol>
     */
    public ReportOutcome reportResult(String taskId, String deviceId, boolean success, String failReason) {
        ReportOutcome[] outcome = new ReportOutcome[1];
        tasks.compute(taskId, (id, task) -> {
            if (task == null) {
                log.warn("[report] 任务不存在: taskId={}, deviceId={}", id, deviceId);
                outcome[0] = ReportOutcome.REJECTED_UNKNOWN;
                return null;
            }
            DeviceUpgradeRecord record = task.findRecord(deviceId);
            if (record == null) {
                log.warn("[report] 设备不在任务中: taskId={}, deviceId={}", id, deviceId);
                outcome[0] = ReportOutcome.REJECTED_UNKNOWN;
                return task;
            }
            if (task.isAbortingOrAborted()) {
                log.info("[report] 任务已中止, 上报被拒绝: taskId={}, deviceId={}, taskStatus={}",
                        id, deviceId, task.getStatus());
                outcome[0] = ReportOutcome.REJECTED_TASK_ABORTED;
                return task;
            }
            if (task.getStatus() == TaskStatus.PAUSED) {
                log.info("[report] 任务已暂停, 上报被拒绝: taskId={}, deviceId={}, pauseReason={}",
                        id, deviceId, task.getPauseReason());
                outcome[0] = ReportOutcome.REJECTED_TASK_PAUSED;
                return task;
            }
            if (record.getState() != DeviceUpgradeState.PENDING) {
                log.info("[report] 重复上报被忽略(不重复计数): taskId={}, deviceId={}, 当前状态={}",
                        id, deviceId, record.getState());
                outcome[0] = ReportOutcome.DUPLICATE_IGNORED;
                return task;
            }
            if (record.getBatchIndex() > task.getCurrentBatchIndex()) {
                log.info("[report] 批次未开始, 上报被拒绝(防跳批): taskId={}, deviceId={}, recordBatch={}, currentBatch={}",
                        id, deviceId, record.getBatchIndex(), task.getCurrentBatchIndex());
                outcome[0] = ReportOutcome.REJECTED_BATCH_NOT_STARTED;
                return task;
            }
            if (success) {
                record.markSuccess();
                deviceRegistry.updateEffectiveVersion(deviceId, task.getTargetVersion());
                log.info("[report] 升级成功: taskId={}, deviceId={}, 生效版本 -> {}",
                        id, deviceId, task.getTargetVersion());
                outcome[0] = ReportOutcome.ACCEPTED_SUCCESS;
            } else {
                record.markFailed(failReason);
                log.info("[report] 升级失败: taskId={}, deviceId={}, attempts={}/{}, 原因={}",
                        id, deviceId, record.getAttempts(), task.getMaxAttemptsPerDevice(), failReason);
                outcome[0] = ReportOutcome.ACCEPTED_FAILURE;
                // 达到每设备尝试次数上限：不再允许重试，直接自动回滚到升级前版本
                if (task.getMaxAttemptsPerDevice() > 0
                        && record.getAttempts() >= task.getMaxAttemptsPerDevice()
                        && record.markRolledBackWithReason(
                                "retry-limit-exceeded: attempts=" + record.getAttempts()
                                        + "/" + task.getMaxAttemptsPerDevice()
                                        + ", lastFailReason=" + failReason)) {
                    deviceRegistry.updateEffectiveVersion(deviceId, record.getPreviousVersion());
                    log.warn("[report] 失败已达尝试上限, 自动回滚: taskId={}, deviceId={}, attempts={}/{}, 生效版本恢复为 {}",
                            id, deviceId, record.getAttempts(), task.getMaxAttemptsPerDevice(),
                            record.getPreviousVersion());
                }
            }
            settle(taskId, task);
            return task;
        });
        return outcome[0];
    }

    /** 上报成功/失败的便捷重载。 */
    public ReportOutcome reportSuccess(String taskId, String deviceId) {
        return reportResult(taskId, deviceId, true, null);
    }

    public ReportOutcome reportFailure(String taskId, String deviceId, String reason) {
        return reportResult(taskId, deviceId, false, reason);
    }

    /**
     * 显式推进批次（与自动推进同一判定：当前批成功数达到阈值才允许进入下一批）。
     *
     * @return true 表示批次发生推进或任务完成
     */
    public boolean advanceBatch(String taskId) {
        boolean[] advanced = new boolean[1];
        tasks.computeIfPresent(taskId, (id, task) -> {
            advanced[0] = task.advanceBatch();

            String criterion = task.getBatchSuccessMode() == BatchSuccessMode.RATIO
                    ? "成功占比=" + task.currentBatchSuccessCount() + "/" + task.currentBatch().size()
                        + "(" + String.format("%.4f", task.currentBatchSuccessRatio())
                        + ") >= ratio=" + task.getBatchSuccessRatio()
                    : "成功台数=" + task.currentBatchSuccessCount() + " >= threshold=" + task.getSuccessThreshold();
            log.info("[advance] taskId={}, 判定依据[{}]: {} -> {}, 任务状态={}",
                    id, task.getBatchSuccessMode(), criterion,
                    advanced[0] ? "推进" : "保持", task.getStatus());
            return task;
        });
        return advanced[0];
    }

    /**
     * 重试失败设备：FAILED -> PENDING，允许设备重新上报。
     * 失败记录清零后若任务处于暂停则自动恢复。
     *
     * <p>重试上限：任务配置了 maxAttemptsPerDevice 且该设备 attempts 已达上限时
     * （失败上报时本应已自动回滚），重试被拒绝，避免突破上限无限重试。</p>
     */
    public boolean retryDevice(String taskId, String deviceId) {
        boolean[] retried = new boolean[1];
        tasks.computeIfPresent(taskId, (id, task) -> {
            DeviceUpgradeRecord record = task.findRecord(deviceId);
            if (record == null) {
                log.info("[retry] 无需重试(无记录): taskId={}, deviceId={}", id, deviceId);
                return task;
            }
            if (task.getMaxAttemptsPerDevice() > 0
                    && record.getAttempts() >= task.getMaxAttemptsPerDevice()
                    && record.getState() == DeviceUpgradeState.FAILED) {
                log.warn("[retry] 重试被拒绝(已达尝试上限): taskId={}, deviceId={}, attempts={}/{}",
                        id, deviceId, record.getAttempts(), task.getMaxAttemptsPerDevice());
                return task;
            }
            if (record.resetForRetry()) {
                retried[0] = true;
                log.info("[retry] 失败设备重置为PENDING: taskId={}, deviceId={}, attempts={}",
                        id, deviceId, record.getAttempts());
                resumeIfResolved(task);
            } else {
                log.info("[retry] 无需重试(非FAILED状态): taskId={}, deviceId={}, 状态={}",
                        id, deviceId, record.getState());
            }
            return task;
        });
        return retried[0];
    }

    /**
     * 回滚失败设备：FAILED -> ROLLED_BACK，设备生效版本恢复到升级前版本。
     * 生效版本字段唯一，回滚在任务锁内原子完成，因此回滚后版本唯一且确定。
     */
    public boolean rollbackDevice(String taskId, String deviceId) {
        boolean[] rolledBack = new boolean[1];
        tasks.computeIfPresent(taskId, (id, task) -> {
            DeviceUpgradeRecord record = task.findRecord(deviceId);
            if (record != null && record.markRolledBack()) {
                deviceRegistry.updateEffectiveVersion(deviceId, record.getPreviousVersion());
                rolledBack[0] = true;
                log.info("[rollback] 设备回滚: taskId={}, deviceId={}, 生效版本恢复为 {}",
                        id, deviceId, record.getPreviousVersion());
                resumeIfResolved(task);
            } else {
                log.info("[rollback] 无需回滚(非FAILED状态): taskId={}, deviceId={}, 状态={}",
                        id, deviceId, record == null ? "无记录" : record.getState());
            }
            return task;
        });
        return rolledBack[0];
    }

    /** 回滚任务内全部失败设备。 */
    public int rollbackAllFailed(String taskId) {
        int[] count = new int[1];
        tasks.computeIfPresent(taskId, (id, task) -> {
            for (Map<String, DeviceUpgradeRecord> batch : task.getBatches()) {
                for (DeviceUpgradeRecord record : batch.values()) {
                    if (record.markRolledBack()) {
                        deviceRegistry.updateEffectiveVersion(record.getDeviceId(), record.getPreviousVersion());
                        count[0]++;
                        log.info("[rollback] 设备回滚: taskId={}, deviceId={}, 生效版本恢复为 {}",
                                id, record.getDeviceId(), record.getPreviousVersion());
                    }
                }
            }
            resumeIfResolved(task);
            return task;
        });
        return count[0];
    }

    /**
     * 离线设备重新上线后的收敛：对处于 RUNNING 任务中、记录仍为 PENDING
     * 且生效版本落后于任务目标版本的设备，重新下发升级命令（保持 PENDING 等待上报）。
     *
     * <p>收敛保证：设备生效版本唯一（单一字段、任务锁内更新），
     * 重连后只会向任务当前目标版本收敛，不会出现两个生效版本或长期中间态。</p>
     *
     * @return 需要重新下发命令的 (taskId -> targetVersion) 列表
     */
    public List<String> convergeOnReconnect(String deviceId) {
        List<String> commands = new ArrayList<>();
        deviceRegistry.find(deviceId).ifPresent(device -> {
            for (UpgradeTask task : tasks.values()) {
                if (task.getStatus() != TaskStatus.RUNNING) {
                    continue;
                }
                DeviceUpgradeRecord record = task.findRecord(deviceId);
                if (record != null && record.isPending()
                        && !task.getTargetVersion().equals(device.getEffectiveVersion())) {
                    commands.add(task.getTaskId() + "->" + task.getTargetVersion());
                    log.info("[converge] 设备重连收敛: deviceId={}, 当前版本={}, 目标版本={}, taskId={}, 重新下发升级命令",
                            deviceId, device.getEffectiveVersion(), task.getTargetVersion(), task.getTaskId());
                }
            }
        });
        return commands;
    }

    public Optional<UpgradeTask> findTask(String taskId) {
        return Optional.ofNullable(tasks.get(taskId));
    }

    /**
     * 一键中止整个任务并全量回滚。整个过程在任务级锁内原子完成：
     * <ul>
     *   <li>RUNNING/PAUSED -> ABORTING -> ABORTED（终态，幂等：重复中止返回 false）；</li>
     *   <li>已升级成功（SUCCESS）的设备：状态 -> ROLLED_BACK，生效版本恢复为各自升级前版本；</li>
     *   <li>当前失败（FAILED）的设备：状态 -> ROLLED_BACK，生效版本恢复为升级前版本；</li>
     *   <li>尚未升级（PENDING）的设备：状态 -> SKIPPED，版本保持原样、不再升级；</li>
     *   <li>与设备上报并发时：二者经同一把任务锁串行。若中止先发生，上报被
     *       {@code REJECTED_TASK_ABORTED} 拒绝且版本不动；若上报先发生（设备已成功），
     *       中止会把它回滚。最终结果唯一确定，不会版本错配或重复回滚。</li>
     * </ul>
     *
     * @return true 表示本次调用真正执行了中止；false 表示任务不存在、已完成或已中止
     */
    public boolean abortTask(String taskId) {
        boolean[] aborted = new boolean[1];
        tasks.computeIfPresent(taskId, (id, task) -> {
            if (!task.beginAbort()) {
                log.info("[abort] 中止被忽略(任务非RUNNING/PAUSED或已中止): taskId={}, taskStatus={}",
                        id, task.getStatus());
                return task;
            }
            log.warn("[abort] 开始整任务中止回滚: taskId={}, targetVersion={}", id, task.getTargetVersion());
            int rolledBack = 0;
            int skipped = 0;
            for (Map<String, DeviceUpgradeRecord> batch : task.getBatches()) {
                for (DeviceUpgradeRecord record : batch.values()) {
                    String deviceId = record.getDeviceId();
                    if (record.markRolledBackFromSuccess("task-aborted: rollback to previous version")) {
                        deviceRegistry.updateEffectiveVersion(deviceId, record.getPreviousVersion());
                        rolledBack++;
                        log.info("[abort] 已成功设备随任务回滚: taskId={}, deviceId={}, 生效版本恢复为 {}",
                                id, deviceId, record.getPreviousVersion());
                    } else if (record.markRolledBackWithReason(
                            "task-aborted: failed device rollback to previous version")) {
                        deviceRegistry.updateEffectiveVersion(deviceId, record.getPreviousVersion());
                        rolledBack++;
                        log.info("[abort] 失败设备随任务回滚: taskId={}, deviceId={}, 生效版本恢复为 {}",
                                id, deviceId, record.getPreviousVersion());
                    } else if (record.markSkipped()) {
                        skipped++;
                        log.info("[abort] 未升级设备保持原样: taskId={}, deviceId={}, 版本={}, 状态=SKIPPED",
                                id, deviceId, record.getPreviousVersion());
                    }
                }
            }
            task.finishAbort();
            aborted[0] = true;
            log.warn("[abort] 整任务中止回滚完成: taskId={}, 回滚设备数={}, 跳过设备数={}, 任务状态={}",
                    id, rolledBack, skipped, task.getStatus());
            return task;
        });
        return aborted[0];
    }

    /**
     * 在任务锁内快照读取任务，保证中止回滚期间查询不会观察到
     * “新版本/旧版本混杂”或设备停在半路的瞬态（ABORTING 不会被锁外观察到）。
     */
    public Optional<UpgradeTask> getTaskConsistent(String taskId) {
        Optional<UpgradeTask>[] result = new Optional[1];
        tasks.computeIfPresent(taskId, (id, task) -> {
            result[0] = Optional.of(task);
            return task;
        });
        return result[0] == null ? Optional.empty() : result[0];
    }

    /** 上报/状态变更后的统一收尾：失败率超限则暂停，否则按阈值自动推进。 */
    private void settle(String taskId, UpgradeTask task) {
        if (task.pauseIfFailureRateExceeded()) {
            log.warn("[settle] 失败率超限, 任务暂停: taskId={}, 原因={}", taskId, task.getPauseReason());
            return;
        }
        if (task.advanceBatch()) {
            log.info("[settle] 批次推进: taskId={}, 当前批次={}/{}, 任务状态={}",
                    taskId, task.getCurrentBatchIndex() + 1, task.getBatchCount(), task.getStatus());
        }
    }

    /** 失败记录全部消解（重试/回滚）后自动恢复暂停的任务。 */
    private void resumeIfResolved(UpgradeTask task) {
        if (task.getStatus() != TaskStatus.PAUSED) {
            return;
        }
        boolean anyFailed = task.getBatches().stream()
                .flatMap(b -> b.values().stream())
                .anyMatch(r -> r.getState() == DeviceUpgradeState.FAILED);
        if (!anyFailed && task.resume()) {
            log.info("[resume] 失败记录已消解, 任务恢复: taskId={}", task.getTaskId());
        }
    }

    /** 测试辅助：清空全部任务。 */
    public void clear() {
        tasks.clear();
    }
}
