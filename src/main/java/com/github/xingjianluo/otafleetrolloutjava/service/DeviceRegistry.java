package com.github.xingjianluo.otafleetrolloutjava.service;

import com.github.xingjianluo.otafleetrolloutjava.config.OtaProperties;
import com.github.xingjianluo.otafleetrolloutjava.domain.Device;
import com.github.xingjianluo.otafleetrolloutjava.domain.DeviceStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 设备注册、心跳与在线状态判定（本地内存模拟）。
 *
 * <p>一致性设计：</p>
 * <ul>
 *   <li>同一设备的所有写操作通过 {@link ConcurrentHashMap#compute} 串行化，
 *       并发上报不会互相覆盖；</li>
 *   <li>心跳时间戳“大者生效”单调推进，重复/乱序上报被忽略，
 *       同一设备的最终在线状态只取决于最大心跳时间戳，因此可预期、不抖动；</li>
 *   <li>在线判定是纯函数（now - lastHeartbeatAt &lt;= offlineThresholdMs），
 *       任意入口在同一 now 下得到一致结果。</li>
 * </ul>
 */
@Service
public class DeviceRegistry {

    private static final Logger log = LoggerFactory.getLogger(DeviceRegistry.class);

    private final Map<String, Device> devices = new ConcurrentHashMap<>();
    private final OtaProperties properties;

    public DeviceRegistry(OtaProperties properties) {
        this.properties = properties;
    }

    /**
     * 注册设备。幂等：重复注册保留首次注册信息，不覆盖生效版本。
     *
     * @return 注册后的设备（已存在时返回既有设备）
     */
    public Device register(String deviceId, String initialVersion, long registeredAt) {
        return devices.compute(deviceId, (id, existing) -> {
            if (existing != null) {
                log.info("[register] 重复注册被忽略: deviceId={}, 保持 effectiveVersion={}, lastHeartbeatAt={}",
                        id, existing.getEffectiveVersion(), existing.getLastHeartbeatAt());
                return existing;
            }
            Device device = new Device(id, initialVersion, registeredAt);
            log.info("[register] 新设备注册: deviceId={}, initialVersion={}, at={}",
                    id, initialVersion, registeredAt);
            return device;
        });
    }

    public Device register(String deviceId, String initialVersion) {
        return register(deviceId, initialVersion, System.currentTimeMillis());
    }

    /**
     * 心跳上报。重复、乱序或并发的同设备上报以最大时间戳收敛，不会造成状态抖动。
     *
     * @param heartbeatAt 上报携带的时间戳（epoch millis）
     * @return 更新后的设备；设备未注册时返回 empty（心跳不隐式创建设备）
     */
    public Optional<Device> heartbeat(String deviceId, long heartbeatAt) {
        Device[] result = new Device[1];
        devices.computeIfPresent(deviceId, (id, device) -> {
            boolean advanced = device.advanceHeartbeat(heartbeatAt);
            log.info("[heartbeat] deviceId={}, heartbeatAt={}, 判定依据: max(lastHeartbeatAt={}, heartbeatAt) -> {}{}",
                    id, heartbeatAt, device.getLastHeartbeatAt(), device.getLastHeartbeatAt(),
                    advanced ? " (已推进)" : " (重复/乱序上报, 忽略)");
            result[0] = device;
            return device;
        });
        if (result[0] == null) {
            log.warn("[heartbeat] 未注册设备的心跳被忽略: deviceId={}", deviceId);
        }
        return Optional.ofNullable(result[0]);
    }

    public Optional<Device> heartbeat(String deviceId) {
        return heartbeat(deviceId, System.currentTimeMillis());
    }

    /** 在指定时刻判定设备在线状态（纯函数，各入口结果一致）。 */
    public DeviceStatus statusAt(String deviceId, long now) {
        Device device = devices.get(deviceId);
        if (device == null) {
            return DeviceStatus.OFFLINE;
        }
        return device.statusAt(now, properties.getOfflineThresholdMs());
    }

    /** 以当前系统时间判定在线状态。 */
    public DeviceStatus statusOf(String deviceId) {
        return statusAt(deviceId, System.currentTimeMillis());
    }

    public boolean isOnline(String deviceId, long now) {
        return statusAt(deviceId, now) == DeviceStatus.ONLINE;
    }

    public Optional<Device> find(String deviceId) {
        return Optional.ofNullable(devices.get(deviceId));
    }

    /** 设备生效版本更新（由编排层在升级成功/回滚时调用，调用方需持任务锁）。 */
    public void updateEffectiveVersion(String deviceId, String version) {
        devices.computeIfPresent(deviceId, (id, device) -> {
            log.info("[version] deviceId={}, 生效版本变更: {} -> {}", id, device.getEffectiveVersion(), version);
            device.setEffectiveVersion(version);
            return device;
        });
    }

    /** 测试辅助：清空全部设备。 */
    public void clear() {
        devices.clear();
    }
}
