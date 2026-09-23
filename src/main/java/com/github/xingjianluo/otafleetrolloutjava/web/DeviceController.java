package com.github.xingjianluo.otafleetrolloutjava.web;

import com.github.xingjianluo.otafleetrolloutjava.dto.HeartbeatRequest;
import com.github.xingjianluo.otafleetrolloutjava.dto.RegisterRequest;
import com.github.xingjianluo.otafleetrolloutjava.model.Device;
import com.github.xingjianluo.otafleetrolloutjava.model.DeviceStatus;
import com.github.xingjianluo.otafleetrolloutjava.service.DeviceService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/devices")
public class DeviceController {

    private final DeviceService deviceService;

    public DeviceController(DeviceService deviceService) {
        this.deviceService = deviceService;
    }

    @PostMapping("/register")
    public Map<String, Object> register(@RequestBody RegisterRequest request) {
        Device device = deviceService.register(request.getDeviceId(), request.getInitialVersion());
        return deviceView(device, DeviceStatus.ONLINE);
    }

    @PostMapping("/heartbeat")
    public Map<String, Object> heartbeat(@RequestBody HeartbeatRequest request) {
        Device device = deviceService.heartbeat(request.getDeviceId(),
                request.getCurrentVersion());
        DeviceStatus status = deviceService.status(request.getDeviceId());
        return deviceView(device, status);
    }

    @GetMapping("/{deviceId}")
    public Map<String, Object> get(@PathVariable String deviceId) {
        Device device = deviceService.requireDevice(deviceId);
        return deviceView(device, deviceService.status(deviceId));
    }

    private Map<String, Object> deviceView(Device device, DeviceStatus status) {
        Map<String, Object> view = new LinkedHashMap<>();
        view.put("deviceId", device.getDeviceId());
        view.put("currentVersion", device.getCurrentVersion());
        view.put("lastHeartbeatMillis", device.getLastHeartbeatMillis());
        view.put("status", status.name());
        return view;
    }
}
