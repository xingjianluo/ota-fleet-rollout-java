package com.github.xingjianluo.otafleetrolloutjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * OTA 相关可配置项（application.properties 中以 ota. 为前缀）。
 *
 * <ul>
 *   <li>ota.offline-threshold-ms：离线阈值，超过该时长未收到心跳即判定 OFFLINE；</li>
 *   <li>ota.batch-success-threshold：批次成功阈值（台数），当前批成功数达到该值才允许推进；</li>
 *   <li>ota.max-failure-rate：最大失败率（0~1），超过则任务暂停；</li>
 *   <li>ota.batch-size：创建任务时默认的每批设备数。</li>
 * </ul>
 */
@Component
@ConfigurationProperties(prefix = "ota")
public class OtaProperties {

    /** 离线阈值（毫秒）。 */
    private long offlineThresholdMs = 30_000L;

    /** 批次成功阈值（台数）。 */
    private int batchSuccessThreshold = 1;

    /** 最大失败率（0~1）。 */
    private double maxFailureRate = 0.5d;

    /** 默认每批设备数。 */
    private int batchSize = 10;

    /** 批次达标判定方式：COUNT 固定台数（历史默认）/ RATIO 成功比例。 */
    private String batchSuccessMode = "COUNT";

    /** RATIO 模式下当前批达标所需的最小成功占比（0~1）。 */
    private double batchSuccessRatio = 1.0d;

    /** 每台设备最大尝试次数（首次 + 重试）；&lt;=0 表示不限制（历史默认）。 */
    private int maxAttemptsPerDevice = 0;

    public long getOfflineThresholdMs() {
        return offlineThresholdMs;
    }

    public void setOfflineThresholdMs(long offlineThresholdMs) {
        this.offlineThresholdMs = offlineThresholdMs;
    }

    public int getBatchSuccessThreshold() {
        return batchSuccessThreshold;
    }

    public void setBatchSuccessThreshold(int batchSuccessThreshold) {
        this.batchSuccessThreshold = batchSuccessThreshold;
    }

    public double getMaxFailureRate() {
        return maxFailureRate;
    }

    public void setMaxFailureRate(double maxFailureRate) {
        this.maxFailureRate = maxFailureRate;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public String getBatchSuccessMode() {
        return batchSuccessMode;
    }

    public void setBatchSuccessMode(String batchSuccessMode) {
        this.batchSuccessMode = batchSuccessMode;
    }

    public double getBatchSuccessRatio() {
        return batchSuccessRatio;
    }

    public void setBatchSuccessRatio(double batchSuccessRatio) {
        this.batchSuccessRatio = batchSuccessRatio;
    }

    public int getMaxAttemptsPerDevice() {
        return maxAttemptsPerDevice;
    }

    public void setMaxAttemptsPerDevice(int maxAttemptsPerDevice) {
        this.maxAttemptsPerDevice = maxAttemptsPerDevice;
    }
}
