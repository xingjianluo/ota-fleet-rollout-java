package com.github.xingjianluo.otafleetrolloutjava.model;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 灰度升级任务。
 */
public class Rollout {
    private final String rolloutId;
    private final String targetVersion;
    private final String baseVersion;
    private final List<List<String>> batches;
    private final double successThreshold;
    private final double failureThreshold;
    private final int maxAttempts;

    private volatile RolloutState state = RolloutState.PENDING;
    private volatile int currentBatch = -1;
    private volatile FailureReason failureReason;
    private volatile String failureDetail;
    private final List<BatchProgress> batchProgress = new ArrayList<>();
    /** deviceId -> 设备任务状态 */
    private final Map<String, DeviceTaskState> deviceStates = new LinkedHashMap<>();
    /** 去重键 -> 是否已计入结果，防止重复上报重复计数 */
    private final Map<String, Boolean> countedReports = new LinkedHashMap<>();
    /** deviceId -> 已下发的升级尝试次数（首次下发为 0，每次重试 +1） */
    private final Map<String, Integer> attempts = new ConcurrentHashMap<>();
    /** 应回滚的设备总数。 */
    private volatile int rollbackTotal;
    /** 已回滚成功的设备数。 */
    private volatile int rollbackSucceeded;

    public Rollout(String rolloutId, String targetVersion, String baseVersion,
                   List<List<String>> batches, double successThreshold,
                   double failureThreshold, int maxAttempts) {
        this.rolloutId = rolloutId;
        this.targetVersion = targetVersion;
        this.baseVersion = baseVersion;
        this.batches = batches;
        this.successThreshold = successThreshold;
        this.failureThreshold = failureThreshold;
        this.maxAttempts = maxAttempts;
    }

    public String getRolloutId() { return rolloutId; }
    public String getTargetVersion() { return targetVersion; }
    public String getBaseVersion() { return baseVersion; }
    public List<List<String>> getBatches() { return batches; }
    public double getSuccessThreshold() { return successThreshold; }
    public double getFailureThreshold() { return failureThreshold; }
    public int getMaxAttempts() { return maxAttempts; }
    public RolloutState getState() { return state; }
    public void setState(RolloutState state) { this.state = state; }
    public int getCurrentBatch() { return currentBatch; }
    public void setCurrentBatch(int currentBatch) { this.currentBatch = currentBatch; }
    public FailureReason getFailureReason() { return failureReason; }
    public void setFailureReason(FailureReason failureReason) { this.failureReason = failureReason; }
    public String getFailureDetail() { return failureDetail; }
    public void setFailureDetail(String failureDetail) { this.failureDetail = failureDetail; }
    public List<BatchProgress> getBatchProgress() { return batchProgress; }
    public Map<String, DeviceTaskState> getDeviceStates() { return deviceStates; }
    public Map<String, Boolean> getCountedReports() { return countedReports; }
    public Map<String, Integer> getAttempts() { return attempts; }
    public int getRollbackTotal() { return rollbackTotal; }
    public void setRollbackTotal(int rollbackTotal) { this.rollbackTotal = rollbackTotal; }
    public int getRollbackSucceeded() { return rollbackSucceeded; }
    public void setRollbackSucceeded(int rollbackSucceeded) { this.rollbackSucceeded = rollbackSucceeded; }
}
