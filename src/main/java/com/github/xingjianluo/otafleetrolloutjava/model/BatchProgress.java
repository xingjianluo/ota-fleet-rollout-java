package com.github.xingjianluo.otafleetrolloutjava.model;

/**
 * 单个批次的推进进度。
 */
public class BatchProgress {
    private final int index;
    private int total;
    private int succeeded;
    private int failed;

    public BatchProgress(int index) {
        this.index = index;
    }

    public int getIndex() { return index; }
    public int getTotal() { return total; }
    public void setTotal(int total) { this.total = total; }
    public int getSucceeded() { return succeeded; }
    public void setSucceeded(int succeeded) { this.succeeded = succeeded; }
    public int getFailed() { return failed; }
    public void setFailed(int failed) { this.failed = failed; }
}
