package com.github.xingjianluo.otafleetrolloutjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * OTA 相关可配置项。
 */
@Component
@ConfigurationProperties(prefix = "ota")
public class OtaProperties {
    /** 设备超过该毫秒数没有心跳即判定离线。 */
    private long offlineThresholdMillis = 30_000;

    public long getOfflineThresholdMillis() {
        return offlineThresholdMillis;
    }

    public void setOfflineThresholdMillis(long offlineThresholdMillis) {
        this.offlineThresholdMillis = offlineThresholdMillis;
    }
}
