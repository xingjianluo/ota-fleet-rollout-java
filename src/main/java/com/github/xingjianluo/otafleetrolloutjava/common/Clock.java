package com.github.xingjianluo.otafleetrolloutjava.common;

/**
 * 可注入的时间源，便于测试离线判定。
 */
public interface Clock {
    long nowMillis();
}
