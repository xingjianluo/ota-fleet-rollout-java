package com.github.xingjianluo.otafleetrolloutjava.domain;

/**
 * 设备在某个升级任务中的执行记录。
 *
 * <p>状态机（单向收敛，终态不重复计数）：</p>
 * <pre>
 *   PENDING --上报成功--> SUCCESS（终态，重复上报忽略）
 *   PENDING --上报失败--> FAILED（终态，重复上报忽略，保留失败原因）
 *   FAILED  --重试------> PENDING（清空失败原因，允许再次上报）
 *   FAILED  --回滚------> ROLLED_BACK（终态，设备回到旧版本）
 * </pre>
 *
 * <p>所有状态迁移由
 * {@link com.github.xingjianluo.otafleetrolloutjava.service.RolloutService}
 * 在任务级锁内完成，保证命令重复投递或设备重复上报不会重复计数。</p>
 */
public class DeviceUpgradeRecord {

    private final String deviceId;
    private final String taskId;
    private final int batchIndex;

    /** 本次升级的目标版本。 */
    private final String targetVersion;

    /** 升级前设备的版本（回滚时恢复到此版本）。 */
    private final String previousVersion;

    private volatile DeviceUpgradeState state = DeviceUpgradeState.PENDING;

    /** 失败原因（可追溯），仅 FAILED 状态有意义。 */
    private volatile String failReason;

    /** 已尝试次数（重试会递增），用于观测与审计。 */
    private volatile int attempts;

    /** 转为 ROLLED_BACK 的原因（重试上限自动回滚 / 整任务中止），可追溯。 */
    private volatile String rollbackReason;

    public DeviceUpgradeRecord(String deviceId, String taskId, int batchIndex,
                               String targetVersion, String previousVersion) {
        this.deviceId = deviceId;
        this.taskId = taskId;
        this.batchIndex = batchIndex;
        this.targetVersion = targetVersion;
        this.previousVersion = previousVersion;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public String getTaskId() {
        return taskId;
    }

    public int getBatchIndex() {
        return batchIndex;
    }

    public String getTargetVersion() {
        return targetVersion;
    }

    public String getPreviousVersion() {
        return previousVersion;
    }

    public DeviceUpgradeState getState() {
        return state;
    }

    public String getFailReason() {
        return failReason;
    }

    public int getAttempts() {
        return attempts;
    }

    public String getRollbackReason() {
        return rollbackReason;
    }

    /**
     * 标记成功。仅 PENDING -> SUCCESS 生效；已是终态时返回 false（幂等去重）。
     */
    public boolean markSuccess() {
        if (state != DeviceUpgradeState.PENDING) {
            return false;
        }
        state = DeviceUpgradeState.SUCCESS;
        attempts++;
        return true;
    }

    /**
     * 标记失败并记录原因。仅 PENDING -> FAILED 生效；已是终态时返回 false（幂等去重）。
     */
    public boolean markFailed(String reason) {
        if (state != DeviceUpgradeState.PENDING) {
            return false;
        }
        state = DeviceUpgradeState.FAILED;
        failReason = reason;
        attempts++;
        return true;
    }

    /**
     * 重试：FAILED -> PENDING，清空失败原因，允许设备再次上报。
     */
    public boolean resetForRetry() {
        if (state != DeviceUpgradeState.FAILED) {
            return false;
        }
        state = DeviceUpgradeState.PENDING;
        failReason = null;
        return true;
    }

    /**
     * 回滚：FAILED -> ROLLED_BACK（终态）。
     */
    public boolean markRolledBack() {
        return markRolledBackWithReason(null);
    }

    /**
     * 回滚：FAILED -> ROLLED_BACK（终态），并记录回滚原因（可追溯）。
     *
     * @param reason 回滚原因（如重试上限自动回滚）；null 表示人工回滚
     */
    public boolean markRolledBackWithReason(String reason) {
        if (state != DeviceUpgradeState.FAILED) {
            return false;
        }
        state = DeviceUpgradeState.ROLLED_BACK;
        this.rollbackReason = reason;
        return true;
    }

    /**
     * 整任务中止时，已成功设备随任务回滚：SUCCESS -> ROLLED_BACK（终态），
     * 生效版本由编排层恢复为 previousVersion。重复中止幂等：仅 SUCCESS 可迁移。
     *
     * @param reason 回滚原因（整任务中止），可追溯
     */
    public boolean markRolledBackFromSuccess(String reason) {
        if (state != DeviceUpgradeState.SUCCESS) {
            return false;
        }
        state = DeviceUpgradeState.ROLLED_BACK;
        this.rollbackReason = reason;
        return true;
    }

    /**
     * 整任务中止时，尚未升级的设备：PENDING -> SKIPPED（终态），
     * 不做任何版本变更，设备保持升级前版本。重复中止幂等。
     */
    public boolean markSkipped() {
        if (state != DeviceUpgradeState.PENDING) {
            return false;
        }
        state = DeviceUpgradeState.SKIPPED;
        return true;
    }

    /**
     * 离线重连收敛：将记录重置为 PENDING 以便重新下发目标版本命令。
     * 仅当记录处于 PENDING 且设备版本落后于目标版本时由编排层调用；
     * 此处幂等：非 PENDING 状态不动作。
     */
    public boolean isPending() {
        return state == DeviceUpgradeState.PENDING;
    }

    @Override
    public String toString() {
        return "DeviceUpgradeRecord{deviceId='" + deviceId + "', taskId='" + taskId
                + "', batch=" + batchIndex + ", state=" + state
                + ", target='" + targetVersion + "', previous='" + previousVersion
                + "', attempts=" + attempts + ", failReason='" + failReason + "'}";
    }
}
