package com.github.xingjianluo.otafleetrolloutjava.model;

/**
 * 下发给设备的命令（幂等记录：同一命令重复投递不产生新的待确认动作）。
 */
public class DeviceCommand {
    private final String commandId;
    private final String deviceId;
    private final String rolloutId;
    private final CommandType type;
    private final String targetVersion;
    private final long createTimeMillis;
    private volatile boolean acked;

    public DeviceCommand(String commandId, String deviceId, String rolloutId,
                         CommandType type, String targetVersion, long createTimeMillis) {
        this.commandId = commandId;
        this.deviceId = deviceId;
        this.rolloutId = rolloutId;
        this.type = type;
        this.targetVersion = targetVersion;
        this.createTimeMillis = createTimeMillis;
    }

    public String getCommandId() { return commandId; }
    public String getDeviceId() { return deviceId; }
    public String getRolloutId() { return rolloutId; }
    public CommandType getType() { return type; }
    public String getTargetVersion() { return targetVersion; }
    public long getCreateTimeMillis() { return createTimeMillis; }
    public boolean isAcked() { return acked; }
    public void setAcked(boolean acked) { this.acked = acked; }
}
