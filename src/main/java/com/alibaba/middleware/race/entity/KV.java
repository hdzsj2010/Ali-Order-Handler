package com.alibaba.middleware.race.entity;

import com.alibaba.middleware.race.OrderSystem;

/**
 * Created by lby on 2016/7/19.
 */
public class KV implements Comparable<KV>, OrderSystem.KeyValue {
    String key;
    public String rawValue;
    static private String booleanTrueValue = "true";
    static private String booleanFalseValue = "false";
    boolean isComparableLong = false;
    long longValue;

    public KV(String key, String rawValue) {
        this.key = key;
        this.rawValue = rawValue;
        if (key.equals("createtime") || key.equals("orderid")) {
            isComparableLong = true;
            longValue = Long.parseLong(rawValue);
        }
    }

    public String key() {
        return key;
    }

    public String valueAsString() {
        return rawValue;
    }

    public long valueAsLong() throws OrderSystem.TypeException {
        try {
            return Long.parseLong(rawValue);
        } catch (NumberFormatException e) {
            throw new OrderSystem.TypeException();
        }
    }

    public double valueAsDouble() throws OrderSystem.TypeException {
        try {
            return Double.parseDouble(rawValue);
        } catch (NumberFormatException e) {
            throw new OrderSystem.TypeException();
        }
    }

    public boolean valueAsBoolean() throws OrderSystem.TypeException {
        if (this.rawValue.equals(booleanTrueValue)) {
            return true;
        }
        if (this.rawValue.equals(booleanFalseValue)) {
            return false;
        }
        throw new OrderSystem.TypeException();
    }

    public int compareTo(KV o) {
        if (!this.key().equals(o.key())) {
            throw new RuntimeException("Cannot compare from different key");
        }
        if (isComparableLong) {
//                return  Long.compare(this.longValue, o.longValue);
            return (this.longValue < o.longValue) ? -1 : ((this.longValue == o.longValue) ? 0 : 1);
        }
        return this.rawValue.compareTo(o.rawValue);
    }

    @Override
    public String toString() {
        return "[" + this.key + "]:" + this.rawValue;
    }
}
