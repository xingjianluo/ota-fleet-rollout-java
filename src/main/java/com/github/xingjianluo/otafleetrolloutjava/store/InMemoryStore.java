package com.github.xingjianluo.otafleetrolloutjava.store;

import com.github.xingjianluo.otafleetrolloutjava.model.Device;
import com.github.xingjianluo.otafleetrolloutjava.model.DeviceCommand;
import com.github.xingjianluo.otafleetrolloutjava.model.Rollout;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 本地内存存储：设备、任务、命令。
 */
@Component
public class InMemoryStore {

    private final ConcurrentMap<String, Device> devices = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Rollout> rollouts = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, DeviceCommand> commands = new ConcurrentHashMap<>();

    public Optional<Device> findDevice(String deviceId) {
        return Optional.ofNullable(devices.get(deviceId));
    }

    public Device putDeviceIfAbsent(Device device) {
        return devices.putIfAbsent(device.getDeviceId(), device);
    }

    public Optional<Rollout> findRollout(String rolloutId) {
        return Optional.ofNullable(rollouts.get(rolloutId));
    }

    public void putRollout(Rollout rollout) {
        rollouts.put(rollout.getRolloutId(), rollout);
    }

    /** 返回全部任务（按创建顺序），用于心跳时的上线收敛遍历。 */
    public Collection<Rollout> allRollouts() {
        return rollouts.values();
    }

    public Optional<DeviceCommand> findCommand(String commandId) {
        return Optional.ofNullable(commands.get(commandId));
    }

    public DeviceCommand putCommandIfAbsent(DeviceCommand command) {
        return commands.putIfAbsent(command.getCommandId(), command);
    }

    /** 清除某设备的全部在途命令（测试中用于模拟命令在设备离线期间丢失）。 */
    public void removeCommandsForDevice(String deviceId) {
        commands.values().removeIf(c -> c.getDeviceId().equals(deviceId));
    }
}
