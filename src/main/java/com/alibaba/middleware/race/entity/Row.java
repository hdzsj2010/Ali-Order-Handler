package com.alibaba.middleware.race.entity;

import java.util.HashMap;

/**
 * Created by lby on 2016/7/19.
 */
public class Row extends HashMap<String, KV> {
    public Row() {
        super();
    }

    public Row(KV kv) {
        super();
        this.put(kv.key(), kv);
    }

    public KV getKV(String key) {
        KV kv = this.get(key);
//        if (kv == null) {
//            throw new RuntimeException(key + " is not exist");
//        }
        return kv;
    }

    public Row putKV(String key, String value) {
        KV kv = new KV(key, value);
        this.put(kv.key(), kv);
        return this;
    }

    public Row putKV(String key, long value) {
        KV kv = new KV(key, Long.toString(value));
        this.put(kv.key(), kv);
        return this;
    }
}
