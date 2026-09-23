package com.github.xingjianluo.otafleetrolloutjava.model;

/**
 * 单台设备在单个任务下的升级状态。
 */
public class DeviceTaskState {
    private final String deviceId;
    private final int batchIndex;
    private DevicePhase phase;
    private String lastError;
    /**
     * 当前在途（尚未被结果确认）的命令 ID；用于防止心跳收敛时重复下发。
     * 结果上报后置空。
     */
    private String pendingCommandId;

    public DeviceTaskState(String deviceId, int batchIndex) {
        this.deviceId = deviceId;
        this.batchIndex = batchIndex;
        this.phase = DevicePhase.IDLE;
    }

    public String getDeviceId() { return deviceId; }
    public int getBatchIndex() { return batchIndex; }
    public DevicePhase getPhase() { return phase; }
    public void setPhase(DevicePhase phase) { this.phase = phase; }
    public String getLastError() { return lastError; }
    public void setLastError(String lastError) { this.lastError = lastError; }
    public String getPendingCommandId() { return pendingCommandId; }
    public void setPendingCommandId(String pendingCommandId) { this.pendingCommandId = pendingCommandId; }
}
