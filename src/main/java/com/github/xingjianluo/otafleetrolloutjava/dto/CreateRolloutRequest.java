package com.github.xingjianluo.otafleetrolloutjava.dto;

import java.util.List;

public class CreateRolloutRequest {
    private String rolloutId;
    private String targetVersion;
    private String baseVersion;
    private List<List<String>> batches;
    private Double successThreshold;
    private Double failureThreshold;
    private Integer maxAttempts;
    public String getRolloutId() { return rolloutId; }
    public void setRolloutId(String rolloutId) { this.rolloutId = rolloutId; }
    public String getTargetVersion() { return targetVersion; }
    public void setTargetVersion(String targetVersion) { this.targetVersion = targetVersion; }
    public String getBaseVersion() { return baseVersion; }
    public void setBaseVersion(String baseVersion) { this.baseVersion = baseVersion; }
    public List<List<String>> getBatches() { return batches; }
    public void setBatches(List<List<String>> batches) { this.batches = batches; }
    public Double getSuccessThreshold() { return successThreshold; }
    public void setSuccessThreshold(Double successThreshold) { this.successThreshold = successThreshold; }
    public Double getFailureThreshold() { return failureThreshold; }
    public void setFailureThreshold(Double failureThreshold) { this.failureThreshold = failureThreshold; }
    public Integer getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(Integer maxAttempts) { this.maxAttempts = maxAttempts; }
}
