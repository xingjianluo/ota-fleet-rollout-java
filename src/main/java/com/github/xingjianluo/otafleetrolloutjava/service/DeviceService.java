package com.github.xingjianluo.otafleetrolloutjava.service;

import com.github.xingjianluo.otafleetrolloutjava.common.Clock;
import com.github.xingjianluo.otafleetrolloutjava.config.OtaProperties;
import com.github.xingjianluo.otafleetrolloutjava.model.Device;
import com.github.xingjianluo.otafleetrolloutjava.model.DeviceStatus;
import com.github.xingjianluo.otafleetrolloutjava.store.InMemoryStore;
import com.github.xingjianluo.otafleetrolloutjava.web.ApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 设备注册、心跳与在线状态判定。
 *
 * <p>一致性要点：</p>
 * <ul>
 *   <li>在线状态不存储为可变枚举，而是由「最后心跳时间 + 离线阈值」动态判定，
 *       因此并发心跳不会产生 ONLINE/OFFLINE 互相覆盖的抖动；</li>
 *   <li>同一设备的心跳以设备实例为锁串行化，心跳时间戳单调推进，
 *       乱序（旧时间戳）心跳不会覆盖新心跳；</li>
 *   <li>注册与心跳均幂等：重复注册返回同一设备。</li>
 * </ul>
 */
@Service
public class DeviceService {

    private static final Logger log = LoggerFactory.getLogger(DeviceService.class);

    private final InMemoryStore store;
    private final Clock clock;
    private final OtaProperties properties;
    private final RolloutService rolloutService;

    public DeviceService(InMemoryStore store, Clock clock, OtaProperties properties,
                         RolloutService rolloutService) {
        this.store = store;
        this.clock = clock;
        this.properties = properties;
        this.rolloutService = rolloutService;
    }

    /**
     * 注册设备（幂等）。
     *
     * @param deviceId       设备唯一标识
     * @param initialVersion 初始生效版本，为空记为 unknown
     */
    public Device register(String deviceId, String initialVersion) {
        if (deviceId == null || deviceId.isBlank()) {
            throw ApiException.badRequest("deviceId 不能为空");
        }
        String version = (initialVersion == null || initialVersion.isBlank())
                ? "unknown" : initialVersion.trim();
        Device existing = store.findDevice(deviceId).orElse(null);
        if (existing != null) {
            log.info("[register][输入] deviceId={}, requestedVersion={} [依据] 设备已存在，注册幂等 "
                    + "[结果] 保留原设备, currentVersion={}", deviceId, version, existing.getCurrentVersion());
            return existing;
        }
        Device device = new Device(deviceId, version, clock.nowMillis());
        Device prev = store.putDeviceIfAbsent(device);
        Device result = prev != null ? prev : device;
        log.info("[register][输入] deviceId={}, initialVersion={} [依据] 设备不存在则新建 "
                + "[结果] deviceId={}, currentVersion={}, registeredAt={}",
                deviceId, version, result.getDeviceId(), result.getCurrentVersion(),
                result.getRegisterTimeMillis());
        return result;
    }

    /**
     * 心跳上报。
     *
     * <p>判定依据：心跳时间以服务器接收时刻（可注入的 {@link Clock}）为准，避免设备端
     * 时钟漂移/伪造导致乱序；同一设备上的并发心跳在设备锁内串行，
     * 最终 {@code lastHeartbeatMillis} 必为最后一次处理的服务器时间（单调不减）。</p>
     *
     * @param deviceId        设备 ID
     * @param reportedVersion 设备当前生效版本，可为空；非空时用于上线收敛
     * @return 处理后的设备
     */
    public Device heartbeat(String deviceId, String reportedVersion) {
        Device device = requireDevice(deviceId);
        long serverTime = clock.nowMillis();

        // 设备级锁：同一设备的心跳（以及与之相关的收敛）串行，跨设备不互斥。
        synchronized (device) {
            long before = device.getLastHeartbeatMillis();
            if (serverTime > before) {
                device.setLastHeartbeatMillis(serverTime);
            }
            if (reportedVersion != null && !reportedVersion.isBlank()) {
                String trimmed = reportedVersion.trim();
                if (!trimmed.equals(device.getCurrentVersion())) {
                    log.info("[heartbeat][输入] deviceId={}, reportedVersion={} "
                                    + "[依据] 设备上报生效版本与记录不同，更新版本记录 {} -> {}",
                            deviceId, trimmed, device.getCurrentVersion(), trimmed);
                    device.setCurrentVersion(trimmed);
                }
            }
            DeviceStatus status = statusOf(device, clock.nowMillis());
            log.info("[heartbeat][输入] deviceId={}, serverTs={}, reportedVersion={} "
                            + "[依据] 原最后心跳={}, 离线阈值={}ms [结果] 最后心跳={}, 在线状态={}",
                    deviceId, serverTime, reportedVersion,
                    before, properties.getOfflineThresholdMillis(),
                    device.getLastHeartbeatMillis(), status);

            // 心跳即上线信号：若设备处于任务中间态，触发版本收敛。
            if (status == DeviceStatus.ONLINE) {
                boolean converged = rolloutService.convergeOnReconnect(deviceId);
                if (converged) {
                    log.info("[heartbeat][收敛] deviceId={} 重新上线后偏离任务目标版本，已重新下发收敛命令", deviceId);
                }
            }
            return device;
        }
    }

    public DeviceStatus status(String deviceId) {
        Device device = requireDevice(deviceId);
        synchronized (device) {
            return statusOf(device, clock.nowMillis());
        }
    }

    public Device requireDevice(String deviceId) {
        return store.findDevice(deviceId)
                .orElseThrow(() -> ApiException.notFound("设备不存在: " + deviceId));
    }

    private DeviceStatus statusOf(Device device, long now) {
        long idle = now - device.getLastHeartbeatMillis();
        return idle <= properties.getOfflineThresholdMillis()
                ? DeviceStatus.ONLINE : DeviceStatus.OFFLINE;
    }
}
