package com.alibaba.middleware.race.entity;

import java.util.List;

/**
 * Created by lby on 2016/7/19.
 */
public class ComparableKeys implements Comparable<ComparableKeys> {
    List<String> orderingKeys;
    Row row;

    public ComparableKeys(List<String> orderingKeys, Row row) {
        if (orderingKeys == null || orderingKeys.size() == 0) {
            throw new RuntimeException("Bad ordering keys, there is a bug maybe");
        }
        this.orderingKeys = orderingKeys;
        this.row = row;
    }

    public int compareTo(ComparableKeys o) {
        if (this.orderingKeys.size() != o.orderingKeys.size()) {
            throw new RuntimeException("Bad ordering keys, there is a bug maybe");
        }
        for (String key : orderingKeys) {
            KV a = this.row.get(key);
            KV b = o.row.get(key);
            if (a == null || b == null) {
                throw new RuntimeException("Bad input data: " + key);
            }
            int ret = a.compareTo(b);
            if (ret != 0) {
                return ret;
            }
        }
        return 0;
    }
}
