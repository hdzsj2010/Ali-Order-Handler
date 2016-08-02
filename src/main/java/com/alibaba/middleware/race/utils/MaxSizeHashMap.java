package com.alibaba.middleware.race.utils;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by wa on 2016/7/27.
 */
public class MaxSizeHashMap<K, V> extends LinkedHashMap<K, V> {
    private final int maxSize;

    public MaxSizeHashMap(int maxSize) {
        this.maxSize = maxSize;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() > maxSize;
    }
}