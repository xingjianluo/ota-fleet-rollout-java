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
 *   <li>仅当前批次成功数达到放行门槛（successThreshold 与本批实际台数的较小值，
 *       按台数计）才允许进入下一批——设备数不是每批台数整数倍时，更小的收尾批次
 *       永远无法凑满原阈值，因此门槛按本批实际台数收敛；</li>
 *   <li>终态（SUCCESS/FAILED）只计数一次，重复上报与重复命令投递不会重复计数或跳批；</li>
 *   <li>失败率 = FAILED / (SUCCESS + FAILED)，超过 maxFailureRate 时任务转为 PAUSED，
 *       并保留可追溯的失败原因（pauseReason 汇总各设备失败原因）；</li>
 *   <li>所有批次完成（全部设备到达终态且未暂停）后任务为 COMPLETED；</li>
 *   <li>当前批设备已全部定性（SUCCESS/ROLLED_BACK，无 PENDING/FAILED 未决）但
 *       成功台数仍达不到放行门槛时，该批不再可能发生任何变化，任务明确收尾为
 *       FAILED 终态并记录 finishReason，不会无限期停留在 RUNNING。</li>
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

    /** 任务终态收尾原因（可追溯），仅 COMPLETED/FAILED 终态有意义。 */
    private volatile String finishReason;

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

    public String getFinishReason() {
        return finishReason;
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

    /**
     * 当前批的放行门槛：successThreshold 与本批实际设备数的较小值。
     *
     * <p>设备数不是每批台数整数倍时，最后一个收尾批次可能只有少量设备，
     * 若仍按原 successThreshold 判定则永远不可能达标，任务将永久卡住；
     * 因此收尾批次的门槛按其实际台数收敛（满批时与原规则完全一致）。</p>
     */
    public int effectiveSuccessThreshold() {
        return Math.min(successThreshold, currentBatch().size());
    }

    /** 当前批次是否达到放行门槛，允许推进。 */
    public boolean currentBatchThresholdReached() {
        return currentBatchSuccessCount() >= effectiveSuccessThreshold();
    }

    /**
     * 当前批设备是否已全部定性：每台设备都处于 SUCCESS 或 ROLLED_BACK，
     * 不存在 PENDING（未上报）或 FAILED（尚未重试/回滚消解）的记录。
     * 一旦全部定性，本批结果不再可能发生任何变化。
     */
    public boolean currentBatchAllDecided() {
        return !currentBatch().isEmpty() && currentBatch().values().stream()
                .allMatch(r -> r.getState() == DeviceUpgradeState.SUCCESS
                        || r.getState() == DeviceUpgradeState.ROLLED_BACK);
    }

    /**
     * 推进或收尾当前批次（每次状态变更后在任务锁内调用，判定幂等且确定）：
     *
     * <ol>
     *   <li>成功台数达到放行门槛：非最后一批则推进到下一批；
     *       最后一批且设备全部定性则任务 COMPLETED（批内尚有未决设备时保持 RUNNING 等待）；</li>
     *   <li>未达门槛但本批设备已全部定性（成功的成功、失败的已回滚）：
     *       本批已无任何可等待的变化，任务明确收尾为 FAILED 终态并记录 finishReason；</li>
     *   <li>其余情况保持 RUNNING。</li>
     * </ol>
     *
     * @return true 表示批次发生了推进，或任务进入了 COMPLETED/FAILED 终态
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
            if (currentBatchAllDecided()) {
                finishReason = "last batch " + currentBatchIndex + " reached threshold "
                        + effectiveSuccessThreshold() + " and all devices decided";
                status = TaskStatus.COMPLETED;
                return true;
            }
            return false;
        }
        // 未达门槛：若本批设备已全部定性，则永远不可能再达标，失败收尾，避免任务卡死
        if (currentBatchAllDecided()) {
            finishReason = "batch " + currentBatchIndex + " exhausted: successCount="
                    + currentBatchSuccessCount() + " < requiredThreshold="
                    + effectiveSuccessThreshold()
                    + " (batchSize=" + currentBatch().size()
                    + "), all devices decided (SUCCESS/ROLLED_BACK), rollout cannot proceed";
            status = TaskStatus.FAILED;
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
        finishReason = null;
        return true;
    }

    /** 任务是否已进入终态（COMPLETED/FAILED）：终态不再接受新的结果上报。 */
    public boolean isTerminal() {
        return status == TaskStatus.COMPLETED || status == TaskStatus.FAILED;
    }

    @Override
    public String toString() {
        return "UpgradeTask{taskId='" + taskId + "', targetVersion='" + targetVersion
                + "', status=" + status + ", currentBatch=" + currentBatchIndex
                + "/" + batches.size() + ", pauseReason='" + pauseReason
                + "', finishReason='" + finishReason + "'}";
    }
}
