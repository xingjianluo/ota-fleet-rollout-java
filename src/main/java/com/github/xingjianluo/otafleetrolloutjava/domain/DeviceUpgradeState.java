package com.github.xingjianluo.otafleetrolloutjava.domain;

/**
 * 单设备在任务中的升级状态。
 */
public enum DeviceUpgradeState {
    PENDING,
    SUCCESS,
    FAILED,
    ROLLED_BACK
}
