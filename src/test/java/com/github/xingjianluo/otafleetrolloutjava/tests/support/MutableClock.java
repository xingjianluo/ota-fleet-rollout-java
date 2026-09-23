package com.github.xingjianluo.otafleetrolloutjava.tests.support;

import com.github.xingjianluo.otafleetrolloutjava.common.Clock;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 测试用可控时钟。
 */
public class MutableClock implements Clock {

    private final AtomicLong now = new AtomicLong(1_000_000L);

    @Override
    public long nowMillis() {
        return now.get();
    }

    public void setTime(long t) {
        now.set(t);
    }

    public void advanceMillis(long delta) {
        now.addAndGet(delta);
    }
}
