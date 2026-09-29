package com.github.xingjianluo.otafleetrolloutjava.domain;

/**
 * 升级任务状态。
 */
public enum TaskStatus {
    /** 进行中：当前批次尚未收尾。 */
    RUNNING,
    /** 暂停：失败率超限，等待重试/回滚消解失败记录。 */
    PAUSED,
    /** 完成：全部批次（含收尾批次）达到有效成功阈值并收尾。 */
    COMPLETED,
    /** 失败（终态）：当前批次设备已全部定性但成功台数不足，无法再推进。 */
    FAILED
}
