package com.github.xingjianluo.otafleetrolloutjava.domain;

/**
 * 设备实体（内存模拟）。
 *
 * <p>并发约束：所有字段的读写都必须通过
 * {@link com.github.xingjianluo.otafleetrolloutjava.service.DeviceRegistry}
 * 中按设备维度持有的锁（ConcurrentHashMap.compute）完成，
 * 因此本类本身不需要额外的同步手段。</p>
 *
 * <p>lastHeartbeatAt 采用“时间戳大者生效”的单调更新策略：
 * 重复、乱序或并发的同设备上报只会让该值收敛到最大时间戳，
 * 从而保证同一设备的最终在线状态可预期、不抖动。</p>
 */
public class Device {

    private final String deviceId;

    /** 设备当前生效版本（唯一且确定，任何时刻只有一个生效版本）。 */
    private volatile String effectiveVersion;

    /** 已接收的最大心跳时间戳（epoch millis），单调不减。 */
    private volatile long lastHeartbeatAt;

    public Device(String deviceId, String initialVersion, long registeredAt) {
        this.deviceId = deviceId;
        this.effectiveVersion = initialVersion;
        this.lastHeartbeatAt = registeredAt;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public String getEffectiveVersion() {
        return effectiveVersion;
    }

    public void setEffectiveVersion(String effectiveVersion) {
        this.effectiveVersion = effectiveVersion;
    }

    public long getLastHeartbeatAt() {
        return lastHeartbeatAt;
    }

    /**
     * 单调推进心跳时间戳；乱序（更旧）的时间戳会被忽略。
     *
     * @param heartbeatAt 上报携带的时间戳
     * @return true 表示本次上报推进了时间戳，false 表示为重复/乱序上报被忽略
     */
    public boolean advanceHeartbeat(long heartbeatAt) {
        if (heartbeatAt > this.lastHeartbeatAt) {
            this.lastHeartbeatAt = heartbeatAt;
            return true;
        }
        return false;
    }

    /**
     * 根据离线阈值判定在线状态。判定是纯函数：给定同一 now 与阈值，
     * 任意入口（心跳线程、查询线程、调度线程）得到的结果一致。
     */
    public DeviceStatus statusAt(long now, long offlineThresholdMs) {
        return (now - lastHeartbeatAt) <= offlineThresholdMs
                ? DeviceStatus.ONLINE
                : DeviceStatus.OFFLINE;
    }

    @Override
    public String toString() {
        return "Device{deviceId='" + deviceId + "', effectiveVersion='" + effectiveVersion
                + "', lastHeartbeatAt=" + lastHeartbeatAt + '}';
    }
}
