package com.github.xingjianluo.otafleetrolloutjava.model;

/**
 * 设备实体。
 *
 * <p>在线状态不保存为字段，而由最后心跳时间与离线阈值动态判定，
 * 从而避免并发心跳造成的状态抖动。</p>
 */
public class Device {
    private final String deviceId;
    private String currentVersion;
    private volatile long lastHeartbeatMillis;
    private volatile long registerTimeMillis;

    public Device(String deviceId, String initialVersion, long nowMillis) {
        this.deviceId = deviceId;
        this.currentVersion = initialVersion;
        this.lastHeartbeatMillis = nowMillis;
        this.registerTimeMillis = nowMillis;
    }

    public String getDeviceId() { return deviceId; }
    public String getCurrentVersion() { return currentVersion; }
    public void setCurrentVersion(String currentVersion) { this.currentVersion = currentVersion; }
    public long getLastHeartbeatMillis() { return lastHeartbeatMillis; }
    public void setLastHeartbeatMillis(long t) { this.lastHeartbeatMillis = t; }
    public long getRegisterTimeMillis() { return registerTimeMillis; }
}
