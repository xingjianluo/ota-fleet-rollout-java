package com.github.xingjianluo.otafleetrolloutjava.dto;

public class RegisterRequest {
    private String deviceId;
    private String initialVersion;
    public String getDeviceId() { return deviceId; }
    public void setDeviceId(String deviceId) { this.deviceId = deviceId; }
    public String getInitialVersion() { return initialVersion; }
    public void setInitialVersion(String initialVersion) { this.initialVersion = initialVersion; }
}
