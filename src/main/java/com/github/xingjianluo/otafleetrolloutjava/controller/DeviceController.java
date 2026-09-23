package com.github.xingjianluo.otafleetrolloutjava.controller;

import com.github.xingjianluo.otafleetrolloutjava.domain.Device;
import com.github.xingjianluo.otafleetrolloutjava.domain.DeviceStatus;
import com.github.xingjianluo.otafleetrolloutjava.service.DeviceRegistry;
import com.github.xingjianluo.otafleetrolloutjava.service.RolloutService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 设备侧入口：注册、心跳、在线状态查询。
 * 心跳后会触发离线重连收敛（向任务当前目标版本对齐）。
 */
@RestController
@RequestMapping("/api/devices")
public class DeviceController {

    private final DeviceRegistry deviceRegistry;
    private final RolloutService rolloutService;

    public DeviceController(DeviceRegistry deviceRegistry, RolloutService rolloutService) {
        this.deviceRegistry = deviceRegistry;
        this.rolloutService = rolloutService;
    }

    @PostMapping("/{deviceId}/register")
    public Map<String, Object> register(@PathVariable String deviceId,
                                        @RequestParam String version) {
        Device device = deviceRegistry.register(deviceId, version);
        return deviceView(device, System.currentTimeMillis());
    }

    @PostMapping("/{deviceId}/heartbeat")
    public ResponseEntity<Map<String, Object>> heartbeat(
            @PathVariable String deviceId,
            @RequestParam(required = false) Long at) {
        long heartbeatAt = at != null ? at : System.currentTimeMillis();
        return deviceRegistry.heartbeat(deviceId, heartbeatAt)
                .map(device -> {
                    // 重连收敛：若设备在运行中的任务里落后于目标版本，重新下发命令
                    List<String> commands = rolloutService.convergeOnReconnect(deviceId);
                    Map<String, Object> view = deviceView(device, System.currentTimeMillis());
                    view.put("redeliveredCommands", commands);
                    return ResponseEntity.ok(view);
                })
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/{deviceId}/status")
    public ResponseEntity<Map<String, Object>> status(@PathVariable String deviceId) {
        long now = System.currentTimeMillis();
        return deviceRegistry.find(deviceId)
                .map(device -> ResponseEntity.ok(deviceView(device, now)))
                .orElse(ResponseEntity.notFound().build());
    }

    private Map<String, Object> deviceView(Device device, long now) {
        Map<String, Object> view = new LinkedHashMap<>();
        view.put("deviceId", device.getDeviceId());
        view.put("effectiveVersion", device.getEffectiveVersion());
        view.put("lastHeartbeatAt", device.getLastHeartbeatAt());
        DeviceStatus status = deviceRegistry.statusAt(device.getDeviceId(), now);
        view.put("status", status.name());
        return view;
    }
}
