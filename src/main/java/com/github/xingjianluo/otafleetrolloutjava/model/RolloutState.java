package com.github.xingjianluo.otafleetrolloutjava.model;

/** 升级任务状态。 */
public enum RolloutState {
    /** 已创建未开始。 */
    PENDING,
    /** 灰度推进中。 */
    RUNNING,
    /** 全部批次成功完成。 */
    COMPLETED,
    /** 失败率超限被暂停，保留失败原因，可重试或回滚。 */
    PAUSED,
    /** 回滚中。 */
    ROLLING_BACK,
    /** 回滚完成。 */
    ROLLED_BACK
}
