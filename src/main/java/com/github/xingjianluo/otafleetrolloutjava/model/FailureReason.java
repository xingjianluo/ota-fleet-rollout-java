package com.github.xingjianluo.otafleetrolloutjava.model;

/**
 * 任务暂停/失败的可追溯原因。
 */
public enum FailureReason {
    /** 当前批次失败率超过阈值。 */
    FAILURE_RATE_EXCEEDED,
    /** 手动触发回滚。 */
    MANUAL_ROLLBACK
}
