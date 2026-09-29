package com.github.xingjianluo.otafleetrolloutjava.service;

import com.github.xingjianluo.otafleetrolloutjava.config.OtaProperties;
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
        /** 任务已结束（COMPLETED/FAILED 终态），迟到上报不再接受。 */
        REJECTED_TASK_FINISHED,
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
    public UpgradeTask createTask(String taskId, String targetVersion, List<String> deviceIds,
                                  int batchSize, int successThreshold, double maxFailureRate) {
        return tasks.compute(taskId, (id, existing) -> {
            if (existing != null) {
                log.info("[createTask] 重复创建被忽略: taskId={}, 保持既有任务 {}", id, existing);
                return existing;
            }
            UpgradeTask task = new UpgradeTask(id, targetVersion, successThreshold, maxFailureRate);
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
                            + "successThreshold={}, maxFailureRate={}",
                    id, targetVersion, deviceIds.size(), task.getBatchCount(),
                    successThreshold, maxFailureRate);
            return task;
        });
    }

    /** 使用配置默认值创建任务。 */
    public UpgradeTask createTask(String taskId, String targetVersion, List<String> deviceIds) {
        return createTask(taskId, targetVersion, deviceIds,
                properties.getBatchSize(),
                properties.getBatchSuccessThreshold(),
                properties.getMaxFailureRate());
    }

    /**
     * 设备上报升级结果。
     *
     * <p>判定依据（按序）：</p>
     * <ol>
     *   <li>任务/记录不存在 -> REJECTED_UNKNOWN；</li>
     *   <li>任务已结束（COMPLETED/FAILED） -> REJECTED_TASK_FINISHED（终态不再接受上报）；</li>
     *   <li>任务已暂停 -> REJECTED_TASK_PAUSED（先重试或回滚失败设备）；</li>
     *   <li>记录已终态 -> DUPLICATE_IGNORED（不重复计数）；</li>
     *   <li>记录所在批次晚于当前批 -> REJECTED_BATCH_NOT_STARTED（防跳批）；</li>
     *   <li>否则接受：成功则推进设备生效版本到目标版本，失败则记录原因；
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
            if (task.isTerminal()) {
                log.info("[report] 任务已结束, 上报被拒绝(终态不可变): taskId={}, deviceId={}, taskStatus={}, finishReason={}",
                        id, deviceId, task.getStatus(), task.getFinishReason());
                outcome[0] = ReportOutcome.REJECTED_TASK_FINISHED;
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
                log.info("[report] 升级失败: taskId={}, deviceId={}, 原因={}", id, deviceId, failReason);
                outcome[0] = ReportOutcome.ACCEPTED_FAILURE;
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
            log.info("[advance] taskId={}, 判定依据: currentBatchSuccess={} >= effectiveThreshold={} -> {}, 任务状态={}, 收尾原因={}",
                    id, task.currentBatchSuccessCount(), task.effectiveSuccessThreshold(),
                    advanced[0] ? "推进/收尾" : "保持", task.getStatus(),
                    task.getStatus() == TaskStatus.FAILED ? task.getFinishReason() : "无");
            return task;
        });
        return advanced[0];
    }

    /**
     * 重试失败设备：FAILED -> PENDING，允许设备重新上报。
     * 失败记录清零后若任务处于暂停则自动恢复；恢复后（或任务本就在运行时）
     * 立即重新进行一次批次收尾判定。
     */
    public boolean retryDevice(String taskId, String deviceId) {
        boolean[] retried = new boolean[1];
        tasks.computeIfPresent(taskId, (id, task) -> {
            DeviceUpgradeRecord record = task.findRecord(deviceId);
            if (record != null && record.resetForRetry()) {
                retried[0] = true;
                log.info("[retry] 失败设备重置为PENDING: taskId={}, deviceId={}, attempts={}",
                        id, deviceId, record.getAttempts());
                afterFailureResolved(id, task);
            } else {
                log.info("[retry] 无需重试(非FAILED状态): taskId={}, deviceId={}, 状态={}",
                        id, deviceId, record == null ? "无记录" : record.getState());
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
                afterFailureResolved(id, task);
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
            afterFailureResolved(id, task);
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

    /** 上报/状态变更后的统一收尾：失败率超限则暂停，否则按门槛推进或明确收尾。 */
    private void settle(String taskId, UpgradeTask task) {
        if (task.pauseIfFailureRateExceeded()) {
            log.warn("[settle] 失败率超限, 任务暂停: taskId={}, 原因={}", taskId, task.getPauseReason());
            return;
        }
        if (task.advanceBatch()) {
            if (task.getStatus() == TaskStatus.FAILED) {
                log.warn("[settle] 当前批设备已全部定性但成功台数不足, 任务失败收尾: taskId={}, 原因={}",
                        taskId, task.getFinishReason());
            } else if (task.getStatus() == TaskStatus.COMPLETED) {
                log.info("[settle] 最后一批全部定性且达标, 任务完成: taskId={}, 任务状态={}",
                        taskId, task.getStatus());
            } else {
                log.info("[settle] 批次推进: taskId={}, 当前批次={}/{}, 任务状态={}",
                        taskId, task.getCurrentBatchIndex() + 1, task.getBatchCount(), task.getStatus());
            }
        }
    }

    /**
     * 失败记录被人工消解（重试/回滚）后的统一重新收尾，在任务锁内调用：
     * 任务处于 PAUSED 时，失败记录全部消解则先恢复 RUNNING；
     * 无论恢复与否，只要任务处于 RUNNING 就立即重新判定一次批次
     * （推进/完成/失败收尾），从而覆盖“未触发暂停就直接回滚”的路径，
     * 避免批内设备已全部定性而任务仍停在 RUNNING。
     */
    private void afterFailureResolved(String taskId, UpgradeTask task) {
        resumeIfResolved(taskId, task);
        if (task.getStatus() == TaskStatus.RUNNING) {
            settle(taskId, task);
        }
    }

    /**
     * 失败记录全部消解（重试/回滚）后自动恢复暂停的任务，
     * 并立即重新进行一次收尾判定：恢复后若当前批设备已全部定性，
     * 则按成功台数推进/完成，或明确收尾为 FAILED，不会停在 RUNNING 空等。
     */
    private void resumeIfResolved(String taskId, UpgradeTask task) {
        if (task.getStatus() != TaskStatus.PAUSED) {
            return;
        }
        boolean anyFailed = task.getBatches().stream()
                .flatMap(b -> b.values().stream())
                .anyMatch(r -> r.getState() == DeviceUpgradeState.FAILED);
        if (!anyFailed && task.resume()) {
            log.info("[resume] 失败记录已消解, 任务恢复: taskId={}", taskId);
            settle(taskId, task);
        }
    }

    /** 测试辅助：清空全部任务。 */
    public void clear() {
        tasks.clear();
    }
}
