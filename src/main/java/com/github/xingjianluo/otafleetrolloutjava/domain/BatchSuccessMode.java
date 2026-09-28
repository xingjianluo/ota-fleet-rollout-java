package com.github.xingjianluo.otafleetrolloutjava.domain;

/**
 * 批次达标判定方式。
 * <ul>
 *   <li>{@link #COUNT}：按固定成功台数判定（历史行为，老配置默认）；</li>
 *   <li>{@link #RATIO}：按当前批次内成功设备占比判定（尾巴批次设备较少时也能正常达标）。</li>
 * </ul>
 */
public enum BatchSuccessMode {
    COUNT,
    RATIO
}
