package com.github.xingjianluo.otafleetrolloutjava.model;

/** 设备在线状态（根据最后心跳时间动态判定，不持久化为易抖动字段）。 */
public enum DeviceStatus {
    ONLINE,
    OFFLINE
}
