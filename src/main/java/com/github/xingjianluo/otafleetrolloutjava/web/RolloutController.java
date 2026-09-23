package com.github.xingjianluo.otafleetrolloutjava.web;

import com.github.xingjianluo.otafleetrolloutjava.dto.CommandDispatchRequest;
import com.github.xingjianluo.otafleetrolloutjava.dto.CreateRolloutRequest;
import com.github.xingjianluo.otafleetrolloutjava.dto.DeviceResultRequest;
import com.github.xingjianluo.otafleetrolloutjava.model.Rollout;
import com.github.xingjianluo.otafleetrolloutjava.service.RolloutService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/rollouts")
public class RolloutController {

    private final RolloutService rolloutService;

    public RolloutController(RolloutService rolloutService) {
        this.rolloutService = rolloutService;
    }

    @PostMapping
    public Rollout create(@RequestBody CreateRolloutRequest request) {
        return rolloutService.create(request.getRolloutId(), request.getTargetVersion(),
                request.getBaseVersion(), request.getBatches(), request.getSuccessThreshold(),
                request.getFailureThreshold(), request.getMaxAttempts());
    }

    @GetMapping("/{rolloutId}")
    public Rollout get(@PathVariable String rolloutId) {
        return rolloutService.requireRollout(rolloutId);
    }

    @PostMapping("/{rolloutId}/start")
    public Rollout start(@PathVariable String rolloutId) {
        return rolloutService.start(rolloutId);
    }

    @PostMapping("/{rolloutId}/commands/upgrade")
    public Rollout dispatchUpgrade(@PathVariable String rolloutId,
                                   @RequestBody CommandDispatchRequest request) {
        return rolloutService.dispatchUpgrade(rolloutId, request.getCommandId(), request.getDeviceId());
    }

    @PostMapping("/{rolloutId}/results")
    public Rollout reportResult(@PathVariable String rolloutId,
                                @RequestBody DeviceResultRequest request) {
        return rolloutService.reportResult(rolloutId, request.getDeviceId(), request.isSuccess(),
                request.getVersion(), request.getErrorMessage());
    }

    @PostMapping("/{rolloutId}/commands/retry")
    public Rollout retry(@PathVariable String rolloutId,
                         @RequestBody CommandDispatchRequest request) {
        return rolloutService.retryFailed(rolloutId, request.getCommandId(), request.getDeviceId());
    }

    @PostMapping("/{rolloutId}/resume")
    public Rollout resume(@PathVariable String rolloutId) {
        return rolloutService.resume(rolloutId);
    }

    @PostMapping("/{rolloutId}/rollback")
    public Rollout rollback(@PathVariable String rolloutId,
                            @RequestBody(required = false) CommandDispatchRequest request) {
        String commandId = request != null ? request.getCommandId() : null;
        return rolloutService.rollback(rolloutId, commandId);
    }
}
