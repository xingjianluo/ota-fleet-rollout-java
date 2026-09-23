package com.github.xingjianluo.otafleetrolloutjava.tests;

import com.github.xingjianluo.otafleetrolloutjava.config.OtaProperties;
import com.github.xingjianluo.otafleetrolloutjava.model.Device;
import com.github.xingjianluo.otafleetrolloutjava.model.DeviceStatus;
import com.github.xingjianluo.otafleetrolloutjava.service.DeviceService;
import com.github.xingjianluo.otafleetrolloutjava.service.RolloutService;
import com.github.xingjianluo.otafleetrolloutjava.store.InMemoryStore;
import com.github.xingjianluo.otafleetrolloutjava.tests.support.MutableClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 设备注册与心跳：重复、乱序（线程交错）、并发上报场景下的在线状态一致性测试。
 */
class DeviceHeartbeatConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(DeviceHeartbeatConcurrencyTest.class);

    private MutableClock clock;
    private InMemoryStore store;
    private DeviceService deviceService;

    @BeforeEach
    void setUp() {
        clock = new MutableClock();
        store = new InMemoryStore();
        OtaProperties props = new OtaProperties();
        props.setOfflineThresholdMillis(10_000);
        RolloutService rolloutService = new RolloutService(store, clock);
        deviceService = new DeviceService(store, clock, props, rolloutService);
    }

    @Test
    @DisplayName("重复注册幂等：保留同一设备与初始版本")
    void duplicateRegistrationIsIdempotent() {
        Device d1 = deviceService.register("dev-1", "v1.0");
        Device d2 = deviceService.register("dev-1", "v9.9");
        log.info("[测试输入] 两次注册同一设备 dev-1: v1.0 / v9.9；[判定依据] 注册按 deviceId 幂等；"
                + "[最终状态] 同一实例={}, 生效版本={}", d1 == d2, d2.getCurrentVersion());
        assertTrue(d1 == d2, "重复注册必须返回同一设备实例");
        assertEquals("v1.0", d2.getCurrentVersion());
    }

    @Test
    @DisplayName("心跳时间以服务器为准：阈值内在线，超过阈值离线（状态由时间动态推导）")
    void onlineStatusDerivedFromServerTimeAndThreshold() {
        deviceService.register("dev-1", "v1.0");
        long registeredAt = clock.nowMillis();

        clock.advanceMillis(5_000);
        deviceService.heartbeat("dev-1", null);
        DeviceStatus online = deviceService.status("dev-1");
        log.info("[测试输入] 注册于 {}, {}ms 后心跳, 阈值 10000ms；[判定依据] now-lastHeartbeat<=阈值；[最终状态] {}",
                registeredAt, 5_000, online);
        assertEquals(DeviceStatus.ONLINE, online);

        // 不再发心跳，时间推进超过阈值 -> 自动离线，不需要任何“写状态”操作，因此不可能被并发写抖动覆盖
        clock.advanceMillis(10_001);
        DeviceStatus offline = deviceService.status("dev-1");
        log.info("[测试输入] 距最后心跳静默 {}ms (>10000)；[判定依据] now-lastHeartbeat>阈值；[最终状态] {}", 10_001, offline);
        assertEquals(DeviceStatus.OFFLINE, offline);

        // 重新心跳 -> 立即收敛回在线
        deviceService.heartbeat("dev-1", null);
        DeviceStatus onlineAgain = deviceService.status("dev-1");
        log.info("[测试输入] 离线设备再次心跳；[判定依据] 最后心跳刷新为当前服务器时间；[最终状态] {}", onlineAgain);
        assertEquals(DeviceStatus.ONLINE, onlineAgain);
    }

    @Test
    @DisplayName("并发心跳（交错推进时间）：最后心跳=最后服务器时间，状态始终可预期")
    void concurrentHeartbeatsInterleavedWithTimeAdvance() throws Exception {
        deviceService.register("dev-conc", "v1.0");
        int threads = 16;
        int perThread = 100;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicReference<Throwable> error = new AtomicReference<>();
        // 屏障：确保所有线程在“时钟推进”之后仍会执行心跳，制造交错乱序窗口
        CountDownLatch phase = new CountDownLatch(threads / 2);

        for (int t = 0; t < threads; t++) {
            final boolean earlyHalf = t < threads / 2;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        deviceService.heartbeat("dev-conc", null);
                        if (earlyHalf && i == perThread - 1) {
                            phase.countDown();
                        }
                        if (!earlyHalf && i == 0) {
                            phase.await();
                            clock.advanceMillis(1);
                        }
                    }
                } catch (Throwable e) {
                    error.compareAndSet(null, e);
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS));
        pool.shutdown();
        if (error.get() != null) {
            throw new AssertionError("并发心跳出现异常", error.get());
        }
        Device device = store.findDevice("dev-conc").orElseThrow();
        DeviceStatus status = deviceService.status("dev-conc");
        log.info("[测试输入] {} 线程 x {} 次心跳，时钟在执行交错中被推进；[判定依据] 设备锁串行，服务器时间单调；"
                        + "[最终状态] lastHeartbeat={}, clockNow={}, status={}",
                threads, perThread, device.getLastHeartbeatMillis(), clock.nowMillis(), status);
        // 无论交错顺序如何，最后心跳不超过当前服务器时间，且设备在线
        assertTrue(device.getLastHeartbeatMillis() <= clock.nowMillis());
        assertEquals(clock.nowMillis(), device.getLastHeartbeatMillis());
        assertEquals(DeviceStatus.ONLINE, status);
    }

    @Test
    @DisplayName("并发重复心跳：同一时刻大量重复上报不产生抖动，状态稳定在线")
    void concurrentDuplicateHeartbeatsKeepStableOnline() throws Exception {
        deviceService.register("dev-dup", "v1.0");
        int threads = 32;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicReference<Throwable> error = new AtomicReference<>();
        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < 100; i++) {
                        deviceService.heartbeat("dev-dup", null);
                    }
                } catch (Throwable e) {
                    error.compareAndSet(null, e);
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS));
        pool.shutdown();
        if (error.get() != null) {
            throw new AssertionError("并发重复心跳出现异常", error.get());
        }
        Device device = store.findDevice("dev-dup").orElseThrow();
        DeviceStatus status = deviceService.status("dev-dup");
        log.info("[测试输入] 32 线程 x 100 次心跳（时钟不推进，全部重复时刻）；[判定依据] 重复心跳幂等；[最终状态] lastHeartbeat={}, status={}",
                device.getLastHeartbeatMillis(), status);
        assertEquals(clock.nowMillis(), device.getLastHeartbeatMillis());
        assertEquals(DeviceStatus.ONLINE, status);
    }
}
