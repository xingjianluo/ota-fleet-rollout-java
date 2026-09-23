package com.github.xingjianluo.otafleetrolloutjava.dto;

public class CommandDispatchRequest {
    private String commandId;
    private String deviceId;
    public String getCommandId() { return commandId; }
    public void setCommandId(String commandId) { this.commandId = commandId; }
    public String getDeviceId() { return deviceId; }
    public void setDeviceId(String deviceId) { this.deviceId = deviceId; }
}
