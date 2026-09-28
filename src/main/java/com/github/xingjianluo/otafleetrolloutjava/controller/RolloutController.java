package com.github.xingjianluo.otafleetrolloutjava.controller;

import com.github.xingjianluo.otafleetrolloutjava.domain.BatchSuccessMode;
import com.github.xingjianluo.otafleetrolloutjava.domain.DeviceUpgradeRecord;
import com.github.xingjianluo.otafleetrolloutjava.domain.UpgradeTask;
import com.github.xingjianluo.otafleetrolloutjava.service.RolloutService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 任务侧入口：创建任务、上报结果、批次推进、重试与回滚。
 */
@RestController
@RequestMapping("/api/rollout")
public class RolloutController {

    private final RolloutService rolloutService;

    public RolloutController(RolloutService rolloutService) {
        this.rolloutService = rolloutService;
    }

    /**
     * 创建任务。deviceIds 以逗号分隔；批次参数缺省时使用配置默认值。
     *
     * @param batchSuccessMode COUNT=固定台数（默认，历史行为）；RATIO=按批次成功占比判定
     * @param batchSuccessRatio RATIO 模式下达标所需最小成功占比（0~1），缺省取全局配置
     * @param maxAttemptsPerDevice 每设备最大尝试次数（首次+重试），0/缺省=不限（历史行为）
     */
    @PostMapping("/tasks/{taskId}")
    public Map<String, Object> createTask(@PathVariable String taskId,
                                          @RequestParam String targetVersion,
                                          @RequestParam String deviceIds,
                                          @RequestParam(required = false, defaultValue = "0") int batchSize,
                                          @RequestParam(required = false, defaultValue = "0") int successThreshold,
                                          @RequestParam(required = false, defaultValue = "-1") double maxFailureRate,
                                          @RequestParam(required = false) String batchSuccessMode,
                                          @RequestParam(required = false, defaultValue = "-1") double batchSuccessRatio,
                                          @RequestParam(required = false, defaultValue = "0") int maxAttemptsPerDevice) {
        List<String> ids = Arrays.stream(deviceIds.split(",")).map(String::trim).toList();
        boolean useDefaults = batchSize <= 0 && successThreshold <= 0 && maxFailureRate < 0
                && batchSuccessMode == null && batchSuccessRatio < 0 && maxAttemptsPerDevice <= 0;
        UpgradeTask task;
        if (useDefaults) {
            task = rolloutService.createTask(taskId, targetVersion, ids);
        } else {
            BatchSuccessMode mode = batchSuccessMode != null
                    ? BatchSuccessMode.valueOf(batchSuccessMode.trim().toUpperCase())
                    : BatchSuccessMode.COUNT;
            task = rolloutService.createTask(taskId, targetVersion, ids,
                    batchSize > 0 ? batchSize : 1,
                    successThreshold > 0 ? successThreshold : 1,
                    maxFailureRate >= 0 ? maxFailureRate : 0.5d,
                    mode,
                    batchSuccessRatio >= 0 ? batchSuccessRatio : 1.0d,
                    maxAttemptsPerDevice);
        }
        return taskView(task);
    }

    @PostMapping("/tasks/{taskId}/report")
    public ResponseEntity<Map<String, Object>> report(@PathVariable String taskId,
                                                      @RequestParam String deviceId,
                                                      @RequestParam boolean success,
                                                      @RequestParam(required = false) String failReason) {
        RolloutService.ReportOutcome outcome = rolloutService.reportResult(taskId, deviceId, success, failReason);
        Map<String, Object> view = new LinkedHashMap<>();
        view.put("outcome", outcome.name());
        rolloutService.findTask(taskId).ifPresent(task -> view.put("task", taskView(task)));
        return ResponseEntity.ok(view);
    }

    @PostMapping("/tasks/{taskId}/advance")
    public ResponseEntity<Map<String, Object>> advance(@PathVariable String taskId) {
        boolean advanced = rolloutService.advanceBatch(taskId);
        Map<String, Object> view = new LinkedHashMap<>();
        view.put("advanced", advanced);
        rolloutService.findTask(taskId).ifPresent(task -> view.put("task", taskView(task)));
        return ResponseEntity.ok(view);
    }

    @PostMapping("/tasks/{taskId}/devices/{deviceId}/retry")
    public Map<String, Object> retry(@PathVariable String taskId, @PathVariable String deviceId) {
        boolean retried = rolloutService.retryDevice(taskId, deviceId);
        return Map.of("retried", retried);
    }

    @PostMapping("/tasks/{taskId}/devices/{deviceId}/rollback")
    public Map<String, Object> rollback(@PathVariable String taskId, @PathVariable String deviceId) {
        boolean rolledBack = rolloutService.rollbackDevice(taskId, deviceId);
        return Map.of("rolledBack", rolledBack);
    }

    @PostMapping("/tasks/{taskId}/rollback-all-failed")
    public Map<String, Object> rollbackAllFailed(@PathVariable String taskId) {
        int count = rolloutService.rollbackAllFailed(taskId);
        return Map.of("rolledBackCount", count);
    }

    /** 一键中止整个任务：已成功设备回滚到旧版本，未升级设备保持原样。 */
    @PostMapping("/tasks/{taskId}/abort")
    public Map<String, Object> abort(@PathVariable String taskId) {
        boolean aborted = rolloutService.abortTask(taskId);
        return Map.of("aborted", aborted);
    }

    @GetMapping("/tasks/{taskId}")
    public ResponseEntity<Map<String, Object>> task(@PathVariable String taskId) {
        return rolloutService.getTaskConsistent(taskId)
                .map(task -> ResponseEntity.ok(taskView(task)))
                .orElse(ResponseEntity.notFound().build());
    }

    private Map<String, Object> taskView(UpgradeTask task) {
        Map<String, Object> view = new LinkedHashMap<>();
        view.put("taskId", task.getTaskId());
        view.put("targetVersion", task.getTargetVersion());
        view.put("status", task.getStatus().name());
        view.put("currentBatchIndex", task.getCurrentBatchIndex());
        view.put("batchCount", task.getBatchCount());
        view.put("pauseReason", task.getPauseReason());
        view.put("batchSuccessMode", task.getBatchSuccessMode().name());
        view.put("successThreshold", task.getSuccessThreshold());
        view.put("batchSuccessRatio", task.getBatchSuccessRatio());
        view.put("maxAttemptsPerDevice", task.getMaxAttemptsPerDevice());
        List<Map<String, Object>> batches = new ArrayList<>();
        task.getBatches().forEach(batch -> {
            Map<String, Object> batchView = new LinkedHashMap<>();
            batch.forEach((deviceId, record) -> batchView.put(deviceId, recordView(record)));
            batches.add(batchView);
        });
        view.put("batches", batches);
        return view;
    }

    private Map<String, Object> recordView(DeviceUpgradeRecord record) {
        Map<String, Object> view = new LinkedHashMap<>();
        view.put("state", record.getState().name());
        view.put("targetVersion", record.getTargetVersion());
        view.put("previousVersion", record.getPreviousVersion());
        view.put("attempts", record.getAttempts());
        view.put("failReason", record.getFailReason());
        view.put("rollbackReason", record.getRollbackReason());
        return view;
    }
}
