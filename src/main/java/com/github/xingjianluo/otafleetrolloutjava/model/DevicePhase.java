package com.github.xingjianluo.otafleetrolloutjava.model;

/**
 * 设备在某个任务中的升级阶段。
 */
public enum DevicePhase {
    /** 尚未进入当前批次（等待后续批次，或回滚后回到基线）。 */
    IDLE,
    /** 已下发升级命令，等待结果。 */
    UPGRADING,
    /** 升级成功，生效版本为任务目标版本。 */
    SUCCESS,
    /** 升级失败。 */
    FAILED,
    /** 已下发回滚命令，等待结果。 */
    ROLLING_BACK,
    /** 回滚成功，生效版本回到任务基线版本。 */
    ROLLED_BACK
}
