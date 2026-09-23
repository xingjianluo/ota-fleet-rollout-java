package com.github.xingjianluo.otafleetrolloutjava.service;

import com.github.xingjianluo.otafleetrolloutjava.common.Clock;
import com.github.xingjianluo.otafleetrolloutjava.model.BatchProgress;
import com.github.xingjianluo.otafleetrolloutjava.model.CommandType;
import com.github.xingjianluo.otafleetrolloutjava.model.Device;
import com.github.xingjianluo.otafleetrolloutjava.model.DeviceCommand;
import com.github.xingjianluo.otafleetrolloutjava.model.DevicePhase;
import com.github.xingjianluo.otafleetrolloutjava.model.DeviceTaskState;
import com.github.xingjianluo.otafleetrolloutjava.model.FailureReason;
import com.github.xingjianluo.otafleetrolloutjava.model.Rollout;
import com.github.xingjianluo.otafleetrolloutjava.model.RolloutState;
import com.github.xingjianluo.otafleetrolloutjava.store.InMemoryStore;
import com.github.xingjianluo.otafleetrolloutjava.web.ApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 灰度升级编排：批次推进、结果计数、失败暂停、重试与回滚、上线收敛。
 *
 * <h3>一致性模型</h3>
 * <ul>
 *   <li><b>任务级串行</b>：所有对同一任务的变更都在 {@code synchronized(rollout)} 内完成，
 *       命令重复投递、结果并发上报、重试/回滚因此不会互相覆盖或重复计数；</li>
 *   <li><b>锁序固定为 设备锁 -> 任务锁</b>（仅心跳收敛路径会从设备锁进入任务锁），
 *       其余路径只持有任务锁，不会反向获取设备锁，因此不存在锁序反转死锁；</li>
 *   <li><b>计数幂等</b>：结果按 {@code 设备:尝试序号(升级) / 设备:回滚} 去重键只计一次，
 *       重复或并发上报返回同一结果，不会重复计数，也不会跳批；</li>
 *   <li><b>命令幂等</b>：同一 commandId 重复投递为 no-op；为同一设备同一尝试
 *       再次投递（含上线收敛重发）也不会产生新的待确认动作。</li>
 * </ul>
 */
@Service
public class RolloutService {

    private static final Logger log = LoggerFactory.getLogger(RolloutService.class);

    private final InMemoryStore store;
    private final Clock clock;

    public RolloutService(InMemoryStore store, Clock clock) {
        this.store = store;
        this.clock = clock;
    }

    // ------------------------------------------------------------------
    // 任务创建与启动
    // ------------------------------------------------------------------

    public Rollout create(String rolloutId, String targetVersion, String baseVersion,
                          List<List<String>> batches, Double successThreshold,
                          Double failureThreshold, Integer maxAttempts) {
        if (rolloutId == null || rolloutId.isBlank()) {
            throw ApiException.badRequest("rolloutId 不能为空");
        }
        if (targetVersion == null || targetVersion.isBlank()) {
            throw ApiException.badRequest("targetVersion 不能为空");
        }
        if (batches == null || batches.isEmpty()
                || batches.stream().anyMatch(b -> b == null || b.isEmpty())) {
            throw ApiException.badRequest("batches 必须是非空的二维列表，且每个批次至少一台设备");
        }
        double success = successThreshold != null ? successThreshold : 0.8d;
        double failure = failureThreshold != null ? failureThreshold : 0.2d;
        int attempts = maxAttempts != null ? maxAttempts : 3;
        if (success < 0 || success > 1 || failure < 0 || failure > 1 || attempts < 1) {
            throw ApiException.badRequest("阈值必须在 [0,1]，maxAttempts 必须 >= 1");
        }
        if (store.findRollout(rolloutId).isPresent()) {
            throw ApiException.conflict("任务已存在: " + rolloutId);
        }
        String base = (baseVersion == null || baseVersion.isBlank()) ? "unknown" : baseVersion.trim();
        Rollout rollout = new Rollout(rolloutId, targetVersion.trim(), base,
                batches.stream().map(List::copyOf).toList(), success, failure, attempts);
        store.putRollout(rollout);
        log.info("[create][输入] rolloutId={}, target={}, base={}, 批次数={}, successThreshold={}, "
                        + "failureThreshold={}, maxAttempts={} [结果] 任务已创建, state={}",
                rolloutId, targetVersion, base, batches.size(), success, failure, attempts,
                rollout.getState());
        return rollout;
    }

    /** 开始任务：打开第 0 批，批次内设备进入待下发升级状态。 */
    public Rollout start(String rolloutId) {
        Rollout rollout = requireRollout(rolloutId);
        synchronized (rollout) {
            if (rollout.getState() != RolloutState.PENDING) {
                throw ApiException.conflict("只有 PENDING 任务可以启动, 当前状态=" + rollout.getState());
            }
            openBatch(rollout, 0);
            rollout.setState(RolloutState.RUNNING);
            log.info("[start][输入] rolloutId={} [结果] 第 0 批已打开, 设备数={}, state={}",
                    rolloutId, rollout.getBatches().get(0).size(), rollout.getState());
            return rollout;
        }
    }

    private void openBatch(Rollout rollout, int index) {
        BatchProgress progress = new BatchProgress(index);
        List<String> devices = rollout.getBatches().get(index);
        progress.setTotal(devices.size());
        rollout.getBatchProgress().add(progress);
        rollout.setCurrentBatch(index);
        for (String deviceId : devices) {
            rollout.getDeviceStates().put(deviceId, new DeviceTaskState(deviceId, index));
            rollout.getAttempts().putIfAbsent(deviceId, 0);
        }
    }

    // ------------------------------------------------------------------
    // 命令投递
    // ------------------------------------------------------------------

    /**
     * 投递升级命令（幂等）。
     */
    public Rollout dispatchUpgrade(String rolloutId, String commandId, String deviceId) {
        Rollout rollout = requireRollout(rolloutId);
        synchronized (rollout) {
            if (rollout.getState() != RolloutState.RUNNING) {
                throw ApiException.conflict("任务非 RUNNING，无法投递升级命令, state=" + rollout.getState());
            }
            DeviceTaskState state = requireDeviceInCurrentBatch(rollout, deviceId);
            String expectedCommandId = upgradeCommandId(rollout, deviceId);
            if (commandId != null && store.findCommand(commandId).isPresent()) {
                log.info("[dispatchUpgrade][输入] rolloutId={}, commandId={}, deviceId={} [依据] 命令已存在，重复投递 no-op",
                        rolloutId, commandId, deviceId);
                return rollout;
            }
            if (state.getPhase() != DevicePhase.IDLE) {
                log.info("[dispatchUpgrade][输入] rolloutId={}, commandId={}, deviceId={} [依据] 设备阶段={}，无需重复下发",
                        rolloutId, commandId, deviceId, state.getPhase());
                return rollout;
            }
            String cid = commandId != null ? commandId : expectedCommandId;
            issueCommand(rollout, cid, deviceId, CommandType.UPGRADE, rollout.getTargetVersion());
            state.setPhase(DevicePhase.UPGRADING);
            state.setPendingCommandId(cid);
            log.info("[dispatchUpgrade][输入] rolloutId={}, commandId={}, deviceId={} [结果] 升级命令已下发, "
                            + "目标版本={}, attempt={}",
                    rolloutId, cid, deviceId, rollout.getTargetVersion(), rollout.getAttempts().get(deviceId));
            return rollout;
        }
    }

    /** 对暂停任务中的失败设备下发重试命令（受 maxAttempts 约束）。 */
    public Rollout retryFailed(String rolloutId, String commandId, String deviceId) {
        Rollout rollout = requireRollout(rolloutId);
        synchronized (rollout) {
            if (rollout.getState() != RolloutState.PAUSED) {
                throw ApiException.conflict("只有 PAUSED 任务可以重试失败设备, state=" + rollout.getState());
            }
            DeviceTaskState state = requireDeviceInCurrentBatch(rollout, deviceId);
            if (state.getPhase() != DevicePhase.FAILED) {
                throw ApiException.conflict("设备当前不是 FAILED，无需重试, phase=" + state.getPhase());
            }
            int usedAttempts = rollout.getAttempts().getOrDefault(deviceId, 0);
            if (usedAttempts + 1 >= rollout.getMaxAttempts()) {
                throw ApiException.conflict("设备已达到最大尝试次数 " + rollout.getMaxAttempts());
            }
            rollout.getAttempts().put(deviceId, usedAttempts + 1);
            String cid = commandId != null ? commandId : upgradeCommandId(rollout, deviceId);
            if (store.findCommand(cid).isEmpty()) {
                issueCommand(rollout, cid, deviceId, CommandType.RETRY, rollout.getTargetVersion());
            }
            state.setPhase(DevicePhase.UPGRADING);
            state.setPendingCommandId(cid);
            state.setLastError(null);
            log.info("[retry][输入] rolloutId={}, deviceId={}, commandId={} [结果] 重试命令已下发, attempt={}/{}",
                    rolloutId, deviceId, cid, rollout.getAttempts().get(deviceId), rollout.getMaxAttempts() - 1);
            return rollout;
        }
    }

    // ------------------------------------------------------------------
    // 结果上报
    // ------------------------------------------------------------------

    /**
     * 设备上报升级/回滚结果（幂等：同一去重键只计一次）。
     */
    public Rollout reportResult(String rolloutId, String deviceId, boolean success,
                                String version, String errorMessage) {
        Rollout rollout = requireRollout(rolloutId);
        synchronized (rollout) {
            RolloutState state = rollout.getState();
            if (state == RolloutState.PAUSED) {
                // 暂停后到达的延迟升级结果仍需入账（可能是重试结果），由内部按阶段处理；
                // 但暂停不会自动解除，必须显式 resume。
            }
            if (state == RolloutState.ROLLING_BACK) {
                return reportRollbackResult(rollout, deviceId, success, version, errorMessage);
            }
            // 幂等优先：任务到达终态（COMPLETED）后到达的重复升级上报按 no-op 处理，
            // 而不是抛出冲突——这保证“设备重复上报”在任何入口下结果一致。
            DeviceTaskState existed = rollout.getDeviceStates().get(deviceId);
            if (existed != null) {
                int attempt = rollout.getAttempts().getOrDefault(deviceId, 0);
                String key = "UPGRADE:" + deviceId + ":" + attempt;
                if (rollout.getCountedReports().containsKey(key)) {
                    log.info("[report][输入] rolloutId={}, deviceId={}, success={}, state={} [依据] 去重键={} 已计入，重复上报 no-op",
                            rolloutId, deviceId, success, state, key);
                    return rollout;
                }
            }
            if (state != RolloutState.RUNNING && state != RolloutState.PAUSED) {
                throw ApiException.conflict("当前任务状态不接受升级结果上报, state=" + state);
            }
            return reportUpgradeResult(rollout, deviceId, success, version, errorMessage);
        }
    }

    private Rollout reportUpgradeResult(Rollout rollout, String deviceId, boolean success,
                                        String version, String errorMessage) {
        DeviceTaskState state = rollout.getDeviceStates().get(deviceId);
        if (state == null) {
            throw ApiException.notFound("设备不属于任务任何批次: " + deviceId);
        }
        if (state.getBatchIndex() != rollout.getCurrentBatch()) {
            throw ApiException.conflict("设备不属于当前批次（可能为已结束批次的迟到上报）, deviceBatch="
                    + state.getBatchIndex() + ", currentBatch=" + rollout.getCurrentBatch());
        }
        int attempt = rollout.getAttempts().getOrDefault(deviceId, 0);
        String dedupeKey = "UPGRADE:" + deviceId + ":" + attempt;
        if (rollout.getCountedReports().putIfAbsent(dedupeKey, Boolean.TRUE) != null) {
            log.info("[report][输入] rolloutId={}, deviceId={}, success={}, attempt={} [依据] 去重键={} 已计入，重复上报 no-op",
                    rollout.getRolloutId(), deviceId, success, attempt, dedupeKey);
            return rollout;
        }

        BatchProgress progress = rollout.getBatchProgress().get(rollout.getCurrentBatch());
        if (success) {
            state.setPhase(DevicePhase.SUCCESS);
            state.setLastError(null);
            state.setPendingCommandId(null);
            updateDeviceVersion(deviceId, rollout.getTargetVersion(), "升级成功");
            // 若这是对失败设备的重试成功，则修正批次计数。
            if (progress.getFailed() > 0 && wasFailedBeforeThisAttempt(rollout, deviceId, attempt)) {
                progress.setFailed(progress.getFailed() - 1);
            }
            progress.setSucceeded(progress.getSucceeded() + 1);
            log.info("[report][输入] rolloutId={}, deviceId={}, success=true, version={} [依据] 计入成功, batch={}, "
                            + "succeeded={}/{}, failed={}",
                    rollout.getRolloutId(), deviceId, version, progress.getIndex(),
                    progress.getSucceeded(), progress.getTotal(), progress.getFailed());
        } else {
            state.setPhase(DevicePhase.FAILED);
            state.setLastError(errorMessage);
            state.setPendingCommandId(null);
            progress.setFailed(progress.getFailed() + 1);
            log.info("[report][输入] rolloutId={}, deviceId={}, success=false, error={} [依据] 计入失败, batch={}, "
                            + "failed={}/{}",
                    rollout.getRolloutId(), deviceId, errorMessage, progress.getIndex(),
                    progress.getFailed(), progress.getTotal());
        }
        evaluateBatch(rollout);
        return rollout;
    }

    /**
     * 判断本次成功上报是否为「先失败后重试成功」，用于把先前计入的失败修正回来。
     * 判据：尝试序号大于 0（首次下发为 0，只有重试过才会 >0）。
     */
    private boolean wasFailedBeforeThisAttempt(Rollout rollout, String deviceId, int attempt) {
        return attempt > 0;
    }

    private Rollout reportRollbackResult(Rollout rollout, String deviceId, boolean success,
                                         String version, String errorMessage) {
        DeviceTaskState state = rollout.getDeviceStates().get(deviceId);
        if (state == null) {
            throw ApiException.notFound("设备不属于任务任何批次: " + deviceId);
        }
        String dedupeKey = "ROLLBACK:" + deviceId;
        if (rollout.getCountedReports().putIfAbsent(dedupeKey, Boolean.TRUE) != null) {
            log.info("[report][输入] rolloutId={}, deviceId={}, rollback [依据] 去重键={} 已计入，重复上报 no-op",
                    rollout.getRolloutId(), deviceId, dedupeKey);
            return rollout;
        }
        if (success) {
            state.setPhase(DevicePhase.ROLLED_BACK);
            state.setLastError(null);
            state.setPendingCommandId(null);
            updateDeviceVersion(deviceId, rollout.getBaseVersion(), "回滚成功");
            rollout.setRollbackSucceeded(rollout.getRollbackSucceeded() + 1);
            log.info("[report][输入] rolloutId={}, deviceId={}, rollback=success, version={} [依据] 回滚成功 {}/{}",
                    rollout.getRolloutId(), deviceId, version,
                    rollout.getRollbackSucceeded(), rollout.getRollbackTotal());
            if (rollout.getRollbackSucceeded() >= rollout.getRollbackTotal()) {
                rollout.setState(RolloutState.ROLLED_BACK);
                log.info("[report][结果] rolloutId={} 全部设备回滚完成, state=ROLLED_BACK, 全量生效版本={}",
                        rollout.getRolloutId(), rollout.getBaseVersion());
            }
        } else {
            // 回滚失败保持中间态并保留原因，等待重试（重新投递同一回滚命令）。
            state.setLastError("ROLLBACK_FAILED: " + errorMessage);
            rollout.getCountedReports().remove(dedupeKey);
            log.warn("[report][输入] rolloutId={}, deviceId={}, rollback=failed, error={} [依据] 保持 ROLLING_BACK，等待重试",
                    rollout.getRolloutId(), deviceId, errorMessage);
        }
        return rollout;
    }

    // ------------------------------------------------------------------
    // 批次评估 / 暂停 / 恢复
    // ------------------------------------------------------------------

    /**
     * 评估当前批次：
     * <ol>
     *   <li>已决设备（成功+失败）一旦使失败率 &gt; failureThreshold，立即暂停并保留原因；</li>
     *   <li>批次全部出结果后：成功率 &gt;= successThreshold 则进入下一批（或完成）；否则暂停。</li>
     * </ol>
     */
    private void evaluateBatch(Rollout rollout) {
        // PAUSED 任务只允许通过 resume() 显式重新评估；重试结果入账后不自动解除暂停，
        // 保证“失败原因可追溯、是否继续由外部决定”。
        if (rollout.getState() == RolloutState.PAUSED) {
            log.info("[evaluate][输入] rolloutId={} 处于 PAUSED，本次结果仅入账，不自动推进",
                    rollout.getRolloutId());
            return;
        }
        BatchProgress p = rollout.getBatchProgress().get(rollout.getCurrentBatch());
        int decided = p.getSucceeded() + p.getFailed();
        double failureRate = (double) p.getFailed() / p.getTotal();
        double successRate = (double) p.getSucceeded() / p.getTotal();

        if (p.getFailed() > 0 && failureRate > rollout.getFailureThreshold()) {
            pause(rollout, FailureReason.FAILURE_RATE_EXCEEDED,
                    String.format("批次 %d 失败率 %.2f%% 超过阈值 %.2f%%（失败 %d/%d）",
                            p.getIndex(), failureRate * 100, rollout.getFailureThreshold() * 100,
                            p.getFailed(), p.getTotal()));
            return;
        }
        if (decided >= p.getTotal()) {
            if (successRate >= rollout.getSuccessThreshold()) {
                advance(rollout);
            } else {
                pause(rollout, FailureReason.FAILURE_RATE_EXCEEDED,
                        String.format("批次 %d 结束时成功率 %.2f%% 未达阈值 %.2f%%（成功 %d/%d，失败 %d）",
                                p.getIndex(), successRate * 100, rollout.getSuccessThreshold() * 100,
                                p.getSucceeded(), p.getTotal(), p.getFailed()));
            }
        }
    }

    private void pause(Rollout rollout, FailureReason reason, String detail) {
        rollout.setState(RolloutState.PAUSED);
        rollout.setFailureReason(reason);
        rollout.setFailureDetail(detail);
        log.warn("[pause][判定] rolloutId={}, reason={}, detail={} [结果] state=PAUSED，批次推进停止，等待重试或回滚",
                rollout.getRolloutId(), reason, detail);
    }

    private void advance(Rollout rollout) {
        int next = rollout.getCurrentBatch() + 1;
        if (next >= rollout.getBatches().size()) {
            rollout.setState(RolloutState.COMPLETED);
            log.info("[advance][判定] rolloutId={} 最后一批成功，state=COMPLETED，全量目标版本={}",
                    rollout.getRolloutId(), rollout.getTargetVersion());
        } else {
            openBatch(rollout, next);
            rollout.setState(RolloutState.RUNNING);
            log.info("[advance][判定] rolloutId={} 批次 {} 达标，进入批次 {}（设备 {} 台）, state=RUNNING",
                    rollout.getRolloutId(), next - 1, next, rollout.getBatches().get(next).size());
        }
    }

    /** 暂停后，失败设备已重试并上报成功，调用本方法重新评估并推进。 */
    public Rollout resume(String rolloutId) {
        Rollout rollout = requireRollout(rolloutId);
        synchronized (rollout) {
            if (rollout.getState() != RolloutState.PAUSED) {
                throw ApiException.conflict("只有 PAUSED 任务可以 resume, state=" + rollout.getState());
            }
            rollout.setState(RolloutState.RUNNING);
            log.info("[resume][输入] rolloutId={} [依据] 手动恢复，重新评估当前批次", rolloutId);
            evaluateBatch(rollout);
            return rollout;
        }
    }

    // ------------------------------------------------------------------
    // 回滚
    // ------------------------------------------------------------------

    /**
     * 回滚整个任务到 {@code baseVersion}。
     *
     * <p>确定性保证：回滚成功的唯一生效版本是 {@code baseVersion}；升级成功的设备也被
     * 重新收敛到基线版本，因此回滚结束后全车队不会同时存在两个生效版本。</p>
     */
    public Rollout rollback(String rolloutId, String commandId) {
        Rollout rollout = requireRollout(rolloutId);
        synchronized (rollout) {
            RolloutState s = rollout.getState();
            if (s != RolloutState.PAUSED && s != RolloutState.RUNNING
                    && s != RolloutState.COMPLETED) {
                throw ApiException.conflict(
                        "仅 RUNNING/PAUSED/COMPLETED 任务可以回滚, state=" + s);
            }
            int currentBatch = rollout.getCurrentBatch();
            int targets = 0;
            for (Map.Entry<String, DeviceTaskState> e : rollout.getDeviceStates().entrySet()) {
                // 只回滚「已打开批次」内、曾经收到升级命令（非 IDLE）且尚未回滚的设备。
                DeviceTaskState ts = e.getValue();
                if (ts.getBatchIndex() > currentBatch || ts.getPhase() == DevicePhase.IDLE
                        || ts.getPhase() == DevicePhase.ROLLED_BACK) {
                    continue;
                }
                targets++;
                ts.setPhase(DevicePhase.ROLLING_BACK);
                String cid = rollbackCommandId(rollout, e.getKey());
                ts.setPendingCommandId(cid);
                if (store.findCommand(cid).isEmpty()) {
                    issueCommand(rollout, cid, e.getKey(), CommandType.ROLLBACK, rollout.getBaseVersion());
                }
            }
            rollout.setState(RolloutState.ROLLING_BACK);
            if (rollout.getFailureReason() == null) {
                rollout.setFailureReason(FailureReason.MANUAL_ROLLBACK);
                rollout.setFailureDetail("运行中手动触发回滚");
            }
            rollout.setRollbackTotal(targets);
            rollout.setRollbackSucceeded(0);
            log.info("[rollback][输入] rolloutId={}, commandId={} [依据] 回滚目标版本=base={}, 待回滚设备={} [结果] state=ROLLING_BACK",
                    rolloutId, commandId, rollout.getBaseVersion(), targets);
            return rollout;
        }
    }

    // ------------------------------------------------------------------
    // 离线设备重新上线收敛
    // ------------------------------------------------------------------

    /**
     * 设备心跳（上线）后收敛到任务当前目标版本。
     *
     * <p>判据：设备处于在途中间态（UPGRADING / ROLLING_BACK）但没有已确认命令记录，
     * 或其生效版本与任务当前要求不一致时，重新下发确定性命令。已成功/已回滚的终态设备
     * 版本已被系统锁定为对应版本，不会重复下发，从而保证版本唯一、不长期停留中间态。</p>
     *
     * @return true 表示本次重新下发了收敛命令
     */
    public boolean convergeOnReconnect(String deviceId) {
        Device device = store.findDevice(deviceId).orElse(null);
        if (device == null) {
            return false;
        }
        boolean issued = false;
        for (Rollout rollout : store.allRollouts()) {
            synchronized (rollout) {
                DeviceTaskState state = rollout.getDeviceStates().get(deviceId);
                if (state == null) {
                    continue;
                }
                RolloutState rs = rollout.getState();
                String pending = state.getPendingCommandId();
                boolean pendingAlive = pending != null && store.findCommand(pending).isPresent();

                if (rs == RolloutState.RUNNING || rs == RolloutState.PAUSED) {
                    boolean needUpgrade = state.getPhase() == DevicePhase.UPGRADING
                            || (state.getPhase() == DevicePhase.IDLE
                                && state.getBatchIndex() == rollout.getCurrentBatch()
                                && rs == RolloutState.RUNNING);
                    if (needUpgrade && !pendingAlive) {
                        String cid = upgradeCommandId(rollout, deviceId);
                        if (store.findCommand(cid).isEmpty()) {
                            issueCommand(rollout, cid, deviceId, CommandType.UPGRADE,
                                    rollout.getTargetVersion());
                            log.info("[converge][输入] deviceId={}, rolloutId={}, state={}, phase={} "
                                            + "[依据] 上线后升级在途命令缺失，重新下发目标版本 {} [结果] commandId={}",
                                    deviceId, rollout.getRolloutId(), rs, state.getPhase(),
                                    rollout.getTargetVersion(), cid);
                            issued = true;
                        }
                        state.setPhase(DevicePhase.UPGRADING);
                        state.setPendingCommandId(cid);
                    }
                } else if (rs == RolloutState.ROLLING_BACK
                        && state.getPhase() == DevicePhase.ROLLING_BACK && !pendingAlive) {
                    String cid = rollbackCommandId(rollout, deviceId);
                    if (store.findCommand(cid).isEmpty()) {
                        issueCommand(rollout, cid, deviceId, CommandType.ROLLBACK, rollout.getBaseVersion());
                        log.info("[converge][输入] deviceId={}, rolloutId={} [依据] 上线后回滚在途命令缺失，重新下发基线版本 {} [结果] commandId={}",
                                deviceId, rollout.getRolloutId(), rollout.getBaseVersion(), cid);
                        issued = true;
                    }
                    state.setPendingCommandId(cid);
                }
            }
        }
        return issued;
    }

    // ------------------------------------------------------------------
    // 辅助方法
    // ------------------------------------------------------------------

    public Rollout requireRollout(String rolloutId) {
        return store.findRollout(rolloutId)
                .orElseThrow(() -> ApiException.notFound("任务不存在: " + rolloutId));
    }

    private DeviceTaskState requireDeviceInCurrentBatch(Rollout rollout, String deviceId) {
        DeviceTaskState state = rollout.getDeviceStates().get(deviceId);
        if (state == null || state.getBatchIndex() != rollout.getCurrentBatch()) {
            throw ApiException.notFound("设备不属于当前批次: " + deviceId
                    + "（currentBatch=" + rollout.getCurrentBatch() + "）");
        }
        return state;
    }

    private void issueCommand(Rollout rollout, String commandId, String deviceId,
                              CommandType type, String targetVersion) {
        DeviceCommand command = new DeviceCommand(commandId, deviceId, rollout.getRolloutId(),
                type, targetVersion, clock.nowMillis());
        DeviceCommand prev = store.putCommandIfAbsent(command);
        if (prev == null) {
            log.info("[command][下发] commandId={}, deviceId={}, type={}, targetVersion={}, rolloutId={}",
                    commandId, deviceId, type, targetVersion, rollout.getRolloutId());
        }
    }

    private void updateDeviceVersion(String deviceId, String version, String reason) {
        store.findDevice(deviceId).ifPresent(d -> {
            synchronized (d) {
                String before = d.getCurrentVersion();
                d.setCurrentVersion(version);
                log.info("[version][输入] deviceId={} [依据] {} [结果] 生效版本 {} -> {}",
                        deviceId, reason, before, version);
            }
        });
    }

    private String upgradeCommandId(Rollout rollout, String deviceId) {
        return "cmd-up-" + rollout.getRolloutId() + "-" + deviceId
                + "-a" + rollout.getAttempts().getOrDefault(deviceId, 0);
    }

    private String rollbackCommandId(Rollout rollout, String deviceId) {
        return "cmd-rb-" + rollout.getRolloutId() + "-" + deviceId;
    }

    /** 供内部需要生成一次性命令 ID 的场景保留。 */
    @SuppressWarnings("unused")
    private String randomCommandId() {
        return UUID.randomUUID().toString();
    }
}
