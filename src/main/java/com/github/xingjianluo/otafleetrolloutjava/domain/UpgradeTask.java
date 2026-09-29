package com.github.xingjianluo.otafleetrolloutjava.domain;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 灰度升级任务：设备被划分为若干批次，按批次推进。
 *
 * <p>推进规则：</p>
 * <ul>
 *   <li>仅当前批次成功数达到有效成功阈值才允许进入下一批；满员批次沿用配置的
 *       successThreshold，收尾批次（设备数不足以凑满一批）的有效阈值降为本批
 *       实际台数（整批全部成功即放行），避免收尾批次永远无法达标；</li>
 *   <li>当前批次成功数不足有效阈值、但本批设备已全部定性
 *       （SUCCESS 或回滚后的 ROLLED_BACK，不存在 PENDING/FAILED）时，
 *       再无补够成功台数的可能，任务落入 FAILED 终态并记录可追溯原因；</li>
 *   <li>终态（SUCCESS/FAILED/ROLLED_BACK）只计数一次，重复上报与重复命令投递
 *       不会重复计数或跳批；</li>
 *   <li>失败率 = FAILED / (SUCCESS + FAILED)，超过 maxFailureRate 时任务转为 PAUSED，
 *       并保留可追溯的失败原因（pauseReason 汇总各设备失败原因）；</li>
 *   <li>所有批次完成（全部设备到达终态且未暂停）后任务为 COMPLETED。</li>
 * </ul>
 *
 * <p>本类的所有公开方法都要求调用方持有任务级锁
 * （{@link com.github.xingjianluo.otafleetrolloutjava.service.RolloutService}
 * 通过 ConcurrentHashMap 按 taskId 串行化），因此内部不做额外同步。</p>
 */
public class UpgradeTask {

    private final String taskId;
    private final String targetVersion;

    /** 进入下一批所需的最小成功台数。 */
    private final int successThreshold;

    /** 允许的最大失败率（0~1），超过则暂停任务。 */
    private final double maxFailureRate;

    /** batches.get(i) 为第 i 批的设备记录（按设备 id 索引）。 */
    private final List<Map<String, DeviceUpgradeRecord>> batches = new ArrayList<>();

    private volatile int currentBatchIndex = 0;
    private volatile TaskStatus status = TaskStatus.RUNNING;

    /** 任务暂停原因（可追溯），仅 PAUSED 状态有意义。 */
    private volatile String pauseReason;

    /** 任务失败原因（可追溯），仅 FAILED 终态有意义。 */
    private volatile String failureReason;

    public UpgradeTask(String taskId, String targetVersion,
                       int successThreshold, double maxFailureRate) {
        this.taskId = taskId;
        this.targetVersion = targetVersion;
        this.successThreshold = successThreshold;
        this.maxFailureRate = maxFailureRate;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getTargetVersion() {
        return targetVersion;
    }

    public int getSuccessThreshold() {
        return successThreshold;
    }

    public double getMaxFailureRate() {
        return maxFailureRate;
    }

    public int getCurrentBatchIndex() {
        return currentBatchIndex;
    }

    public TaskStatus getStatus() {
        return status;
    }

    public String getPauseReason() {
        return pauseReason;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public int getBatchCount() {
        return batches.size();
    }

    /** 追加一个批次（创建任务时调用，调用方需保证任务尚未开始推进）。 */
    public void addBatch(List<DeviceUpgradeRecord> batchRecords) {
        Map<String, DeviceUpgradeRecord> batch = new LinkedHashMap<>();
        for (DeviceUpgradeRecord r : batchRecords) {
            batch.put(r.getDeviceId(), r);
        }
        batches.add(batch);
    }

    public List<Map<String, DeviceUpgradeRecord>> getBatches() {
        return Collections.unmodifiableList(batches);
    }

    public Map<String, DeviceUpgradeRecord> currentBatch() {
        if (currentBatchIndex >= batches.size()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(batches.get(currentBatchIndex));
    }

    /** 查找设备在任务中的记录（可能位于任意批次）。 */
    public DeviceUpgradeRecord findRecord(String deviceId) {
        for (Map<String, DeviceUpgradeRecord> batch : batches) {
            DeviceUpgradeRecord r = batch.get(deviceId);
            if (r != null) {
                return r;
            }
        }
        return null;
    }

    /** 当前批次成功台数（终态 SUCCESS 只计一次，天然幂等）。 */
    public long currentBatchSuccessCount() {
        return currentBatch().values().stream()
                .filter(r -> r.getState() == DeviceUpgradeState.SUCCESS)
                .count();
    }

    /** 当前批次失败台数。 */
    public long currentBatchFailureCount() {
        return currentBatch().values().stream()
                .filter(r -> r.getState() == DeviceUpgradeState.FAILED)
                .count();
    }

    /** 当前批次设备台数。 */
    public int currentBatchSize() {
        return currentBatch().size();
    }

    /**
     * 当前批次的有效成功阈值。
     *
     * <p>满员批次（设备数 &gt;= 配置阈值）沿用原配置阈值，行为与历史一致；
     * 收尾批次（设备数不足以凑满一批，必然也凑不满原阈值）的有效阈值降为
     * 本批实际台数——即整批全部成功才算放行，避免设备数不是每批整数倍时
     * 收尾批次永远无法达标而卡死。</p>
     */
    public int currentBatchEffectiveThreshold() {
        return Math.min(successThreshold, currentBatchSize());
    }

    /** 当前批次是否达到有效成功阈值，允许推进。 */
    public boolean currentBatchThresholdReached() {
        return currentBatchSuccessCount() >= currentBatchEffectiveThreshold();
    }

    /** 当前批次是否仍有未定性设备（PENDING：尚未上报；FAILED：仍可重试/回滚）。 */
    public boolean currentBatchHasUndecided() {
        return currentBatch().values().stream()
                .anyMatch(r -> r.getState() == DeviceUpgradeState.PENDING
                        || r.getState() == DeviceUpgradeState.FAILED);
    }

    /**
     * 推进/收尾当前批次，每次调用只有一个确定结果：
     *
     * <ul>
     *   <li>达到有效成功阈值：非末批推进到下一批；末批则在全部设备定性后
     *       置为 COMPLETED（设备尚未全部定性时保持 RUNNING，等待后续上报）；</li>
     *   <li>未达有效阈值但本批设备已全部定性（SUCCESS/FAILED 已处理为
     *       SUCCESS/ROLLED_BACK）：再无补够成功台数的可能，置为 FAILED 终态
     *       并记录可追溯原因，任务不再无限等待；</li>
     *   <li>其余情况保持 RUNNING。</li>
     * </ul>
     *
     * @return true 表示批次发生了推进，或任务进入 COMPLETED/FAILED 终态
     */
    public boolean advanceBatch() {
        if (status != TaskStatus.RUNNING) {
            return false;
        }
        if (currentBatchThresholdReached()) {
            if (currentBatchIndex < batches.size() - 1) {
                currentBatchIndex++;
                return true;
            }
            // 已是最后一批且达标：仅当本批不存在未决设备时才可完成
            if (!currentBatchHasUndecided()) {
                status = TaskStatus.COMPLETED;
                return true;
            }
            return false;
        }
        // 未达有效阈值：若本批设备已全部定性，再无补够成功台数的可能 -> 失败收尾
        if (!currentBatchHasUndecided()) {
            status = TaskStatus.FAILED;
            failureReason = "batch=" + currentBatchIndex
                    + ", successCount=" + currentBatchSuccessCount()
                    + " < effectiveThreshold=" + currentBatchEffectiveThreshold()
                    + "（配置阈值=" + successThreshold + ", 本批台数=" + currentBatchSize()
                    + "），且本批设备已全部定性(SUCCESS/ROLLED_BACK)，无法放行";
            return true;
        }
        return false;
    }

    /**
     * 失败率超限则暂停任务，并汇总可追溯的失败原因。
     *
     * @return true 表示本次调用触发了暂停
     */
    public boolean pauseIfFailureRateExceeded() {
        if (status != TaskStatus.RUNNING) {
            return false;
        }
        long success = 0;
        long failed = 0;
        List<String> reasons = new ArrayList<>();
        for (Map<String, DeviceUpgradeRecord> batch : batches) {
            for (DeviceUpgradeRecord r : batch.values()) {
                if (r.getState() == DeviceUpgradeState.SUCCESS) {
                    success++;
                } else if (r.getState() == DeviceUpgradeState.FAILED) {
                    failed++;
                    reasons.add(r.getDeviceId() + ":" + r.getFailReason());
                }
            }
        }
        long decided = success + failed;
        if (decided == 0) {
            return false;
        }
        double failureRate = (double) failed / decided;
        if (failureRate > maxFailureRate) {
            status = TaskStatus.PAUSED;
            pauseReason = "failureRate=" + String.format("%.4f", failureRate)
                    + " > maxFailureRate=" + maxFailureRate
                    + ", failedDevices=[" + String.join("; ", reasons) + "]";
            return true;
        }
        return false;
    }

    /** 暂停后人工/策略恢复（重试或回滚处理完失败设备后调用）。 */
    public boolean resume() {
        if (status != TaskStatus.PAUSED) {
            return false;
        }
        status = TaskStatus.RUNNING;
        pauseReason = null;
        return true;
    }

    @Override
    public String toString() {
        return "UpgradeTask{taskId='" + taskId + "', targetVersion='" + targetVersion
                + "', status=" + status + ", currentBatch=" + currentBatchIndex
                + "/" + batches.size() + ", pauseReason='" + pauseReason
                + "', failureReason='" + failureReason + "'}";
    }
}
