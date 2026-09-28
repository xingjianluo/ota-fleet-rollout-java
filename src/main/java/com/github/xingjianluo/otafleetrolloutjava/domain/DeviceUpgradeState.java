package com.github.xingjianluo.otafleetrolloutjava.domain;

/**
 * 单设备在任务中的升级状态。
 * <pre>
 *   PENDING --上报成功--> SUCCESS        （终态）
 *   PENDING --上报失败--> FAILED         （保留 failReason，可重试/回滚）
 *   FAILED  --重试------> PENDING        （attempts 已在失败时递增）
 *   FAILED  --回滚------> ROLLED_BACK    （终态，生效版本恢复 previousVersion）
 *   PENDING --任务中止--> SKIPPED        （终态，尚未升级，版本保持原样）
 *   SUCCESS --任务中止--> ROLLED_BACK    （终态，随整任务回滚退回旧版本）
 * </pre>
 */
public enum DeviceUpgradeState {
    PENDING,
    SUCCESS,
    FAILED,
    ROLLED_BACK,
    /** 任务中止时尚未升级的设备：保持升级前版本，不再升级。 */
    SKIPPED
}
