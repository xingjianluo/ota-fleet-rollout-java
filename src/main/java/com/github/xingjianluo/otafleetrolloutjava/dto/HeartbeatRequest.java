package com.github.xingjianluo.otafleetrolloutjava.dto;

public class HeartbeatRequest {
    private String deviceId;
    /** 可选：设备当前生效版本，用于上线收敛判定。 */
    private String currentVersion;
    public String getDeviceId() { return deviceId; }
    public void setDeviceId(String deviceId) { this.deviceId = deviceId; }
    public String getCurrentVersion() { return currentVersion; }
    public void setCurrentVersion(String currentVersion) { this.currentVersion = currentVersion; }
}
