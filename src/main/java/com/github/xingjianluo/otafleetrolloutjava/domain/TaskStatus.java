package com.github.xingjianluo.otafleetrolloutjava.domain;

/**
 * 升级任务状态。
 * <pre>
 *   RUNNING  --失败率超限--> PAUSED --失败记录消解--> RUNNING
 *   RUNNING  --全部批次完成--> COMPLETED   （终态）
 *   RUNNING/PAUSED --一键中止--> ABORTING --全量回滚完成--> ABORTED （终态）
 * </pre>
 */
public enum TaskStatus {
    RUNNING,
    PAUSED,
    COMPLETED,
    /** 中止回滚进行中：此为瞬态，任务锁外的查询不会观察到该状态。 */
    ABORTING,
    /** 中止回滚完成：成功设备已退回旧版本，未升级设备保持原样（SKIPPED）。 */
    ABORTED
}
