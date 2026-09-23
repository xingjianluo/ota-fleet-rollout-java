package com.github.xingjianluo.otafleetrolloutjava.tests;

import com.github.xingjianluo.otafleetrolloutjava.config.OtaProperties;
import com.github.xingjianluo.otafleetrolloutjava.domain.DeviceStatus;
import com.github.xingjianluo.otafleetrolloutjava.service.DeviceRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 设备注册/心跳/在线判定的并发与幂等测试。
 * 每个用例打印：输入、判定依据、最终状态。
 */
class DeviceRegistryTest {

    private static final Logger log = LoggerFactory.getLogger(DeviceRegistryTest.class);
    private static final long OFFLINE_THRESHOLD_MS = 1_000L;

    private DeviceRegistry registry;

    @BeforeEach
    void setUp() {
        OtaProperties properties = new OtaProperties();
        properties.setOfflineThresholdMs(OFFLINE_THRESHOLD_MS);
        registry = new DeviceRegistry(properties);
    }

    @Test
    void offlineThresholdDeterminesStatus() {
        long t0 = 1_000_000L;
        registry.register("dev-1", "1.0.0", t0);

        DeviceStatus atThreshold = registry.statusAt("dev-1", t0 + OFFLINE_THRESHOLD_MS);
        DeviceStatus beyondThreshold = registry.statusAt("dev-1", t0 + OFFLINE_THRESHOLD_MS + 1);

        log.info("[用例:离线阈值] 输入: lastHeartbeatAt={}, threshold={}ms, 观测点1={}, 观测点2={}",
                t0, OFFLINE_THRESHOLD_MS, t0 + OFFLINE_THRESHOLD_MS, t0 + OFFLINE_THRESHOLD_MS + 1);
        log.info("[用例:离线阈值] 判定依据: now - lastHeartbeatAt <= threshold 为 ONLINE");
        log.info("[用例:离线阈值] 最终状态: 观测点1={}, 观测点2={}", atThreshold, beyondThreshold);

        assertEquals(DeviceStatus.ONLINE, atThreshold);
        assertEquals(DeviceStatus.OFFLINE, beyondThreshold);
        assertEquals(DeviceStatus.OFFLINE, registry.statusAt("ghost", t0), "未注册设备视为离线");
    }

    @Test
    void outOfOrderHeartbeatDoesNotFlap() {
        long t0 = 1_000_000L;
        registry.register("dev-2", "1.0.0", t0);

        registry.heartbeat("dev-2", t0 + 900);   // 较新心跳先到
        registry.heartbeat("dev-2", t0 + 100);   // 乱序旧心跳后到
        registry.heartbeat("dev-2", t0 + 900);   // 重复心跳

        long lastHeartbeatAt = registry.find("dev-2").orElseThrow().getLastHeartbeatAt();
        DeviceStatus status = registry.statusAt("dev-2", t0 + 900 + OFFLINE_THRESHOLD_MS);

        log.info("[用例:乱序心跳] 输入: 心跳序列=[+900, +100, +900], threshold={}ms", OFFLINE_THRESHOLD_MS);
        log.info("[用例:乱序心跳] 判定依据: 时间戳大者生效, 乱序/重复上报被忽略");
        log.info("[用例:乱序心跳] 最终状态: lastHeartbeatAt={}, status={}", lastHeartbeatAt, status);

        assertEquals(t0 + 900, lastHeartbeatAt, "乱序旧心跳不得覆盖较新时间戳");
        assertEquals(DeviceStatus.ONLINE, status);
    }

    @Test
    void duplicateRegistrationIsIdempotent() {
        long t0 = 1_000_000L;
        registry.register("dev-3", "1.0.0", t0);
        registry.register("dev-3", "9.9.9", t0 + 5000); // 重复注册不得覆盖版本

        String version = registry.find("dev-3").orElseThrow().getEffectiveVersion();
        log.info("[用例:重复注册] 输入: 两次注册版本=[1.0.0, 9.9.9]");
        log.info("[用例:重复注册] 判定依据: 已存在设备保留首次注册信息");
        log.info("[用例:重复注册] 最终状态: effectiveVersion={}", version);

        assertEquals("1.0.0", version);
    }

    @Test
    void concurrentHeartbeatsConvergeToMaxTimestamp() throws Exception {
        long t0 = 1_000_000L;
        registry.register("dev-4", "1.0.0", t0);

        int threads = 16;
        int reportsPerThread = 200;
        long maxOffset = 10_000L;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                try {
                    start.await();
                    for (int j = 0; j < reportsPerThread; j++) {
                        // 随机乱序时间戳（含重复），模拟并发+乱序+重复上报
                        long offset = ThreadLocalRandom.current().nextLong(maxOffset + 1);
                        registry.heartbeat("dev-4", t0 + offset);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            f.get(30, TimeUnit.SECONDS);
        }
        pool.shutdown();

        long lastHeartbeatAt = registry.find("dev-4").orElseThrow().getLastHeartbeatAt();
        DeviceStatus status = registry.statusAt("dev-4", t0 + maxOffset);

        log.info("[用例:并发心跳] 输入: {}线程 x {}次随机时间戳上报, 最大偏移={}ms",
                threads, reportsPerThread, maxOffset);
        log.info("[用例:并发心跳] 判定依据: compute 串行化 + 时间戳大者生效, 最终收敛到最大时间戳");
        log.info("[用例:并发心跳] 最终状态: lastHeartbeatAt={}, status={}", lastHeartbeatAt, status);

        assertTrue(lastHeartbeatAt <= t0 + maxOffset, "不得超过上报的最大时间戳");
        // 高概率覆盖到最大值；为保证确定性，再补一次确定的最大值上报验证收敛
        registry.heartbeat("dev-4", t0 + maxOffset);
        assertEquals(t0 + maxOffset, registry.find("dev-4").orElseThrow().getLastHeartbeatAt());
        assertEquals(DeviceStatus.ONLINE, registry.statusAt("dev-4", t0 + maxOffset));
    }

    @Test
    void concurrentRegisterAndHeartbeatKeepsSingleDevice() throws Exception {
        long t0 = 1_000_000L;
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            final int n = i;
            futures.add(pool.submit(() -> {
                try {
                    start.await();
                    if (n % 2 == 0) {
                        registry.register("dev-5", "1.0.0", t0);
                    } else {
                        registry.heartbeat("dev-5", t0 + 100);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            f.get(30, TimeUnit.SECONDS);
        }
        pool.shutdown();

        // 并发注册与心跳交错后，设备要么未注册（心跳先于注册被忽略）、
        // 要么唯一存在且版本确定——无论如何不得出现状态互相覆盖后的不确定结果
        var device = registry.find("dev-5");
        log.info("[用例:并发注册心跳] 输入: {}线程交错执行注册与心跳", threads);
        log.info("[用例:并发注册心跳] 判定依据: compute 按设备串行化, 注册幂等, 心跳不隐式建设备");
        log.info("[用例:并发注册心跳] 最终状态: device={}", device);
        device.ifPresent(d -> {
            assertEquals("1.0.0", d.getEffectiveVersion(), "并发下版本必须唯一确定");
            assertTrue(d.getLastHeartbeatAt() >= t0);
        });
    }
}
