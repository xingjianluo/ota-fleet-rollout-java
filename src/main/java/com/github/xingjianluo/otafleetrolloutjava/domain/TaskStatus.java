package com.github.xingjianluo.otafleetrolloutjava.domain;

/**
 * 升级任务状态。
 *
 * <p>RUNNING/PAUSED 为非终态（PAUSED 处理完失败设备后可恢复 RUNNING）；
 * COMPLETED 与 FAILED 为终态：前者表示全部批次成功收尾，后者表示某一批
 * 设备均已定性但成功台数不足，任务不再可能推进，明确收尾失败。</p>
 */
public enum TaskStatus {
    RUNNING,
    PAUSED,
    COMPLETED,
    /** 终态：当前批设备全部到达定性终态（SUCCESS/ROLLED_BACK）但成功台数不足，任务失败收尾。 */
    FAILED
}
