package com.github.xingjianluo.otafleetrolloutjava.dto;

public class DeviceResultRequest {
    private String deviceId;
    /** 成功时上报的生效版本。 */
    private String version;
    private boolean success;
    private String errorMessage;
    public String getDeviceId() { return deviceId; }
    public void setDeviceId(String deviceId) { this.deviceId = deviceId; }
    public String getVersion() { return version; }
    public void setVersion(String version) { this.version = version; }
    public boolean isSuccess() { return success; }
    public void setSuccess(boolean success) { this.success = success; }
    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
}
