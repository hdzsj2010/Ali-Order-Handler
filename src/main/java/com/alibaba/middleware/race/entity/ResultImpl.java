package com.alibaba.middleware.race.entity;

import com.alibaba.middleware.race.OrderSystem;

import java.util.Set;

/**
 * Created by lby on 2016/7/19.
 */
public class ResultImpl implements OrderSystem.Result {
    private long orderid;
    private Row kvMap;

    private ResultImpl(long orderid, Row kv) {
        this.orderid = orderid;
        this.kvMap = kv;
    }

    //    public static ResultImpl createResultRow(Row orderData, Row buyerData,
//                                             Row goodData, Set<String> queryingKeys) {
//        if (orderData == null || buyerData == null || goodData == null) {
//            throw new RuntimeException("Bad data!");
//        }
//        Row allkv = new Row();
//        long orderid;
//        try {
//            orderid = orderData.get("orderid").valueAsLong();
//        } catch (OrderSystem.TypeException e) {
//            throw new RuntimeException("Bad data!");
//        }
//
//        for (KV kv : orderData.values()) {
//            if (queryingKeys == null || queryingKeys.contains(kv.key)) {
//                allkv.put(kv.key(), kv);
//            }
//        }
//        for (KV kv : buyerData.values()) {
//            if (queryingKeys == null || queryingKeys.contains(kv.key)) {
//                allkv.put(kv.key(), kv);
//            }
//        }
//        for (KV kv : goodData.values()) {
//            if (queryingKeys == null || queryingKeys.contains(kv.key)) {
//                allkv.put(kv.key(), kv);
//            }
//        }
//        return new ResultImpl(orderid, allkv);
//    }
    public static ResultImpl createResultRow(Row orderData, Row buyerData,
                                             Row goodData, Set<String> queryingKeys) {
        if (orderData == null || buyerData == null || goodData == null) {
            throw new RuntimeException("Bad data!");
        }
        Row allkv = new Row();
        long orderid;
        try {
            orderid = orderData.get("orderid").valueAsLong();
        } catch (OrderSystem.TypeException e) {
            throw new RuntimeException("Bad data!");
        }
        if (queryingKeys == null) {
            for (KV kv : orderData.values()) {
                allkv.put(kv.key(), kv);
            }
            for (KV kv : buyerData.values()) {
                allkv.put(kv.key(), kv);
            }
            for (KV kv : goodData.values()) {
                allkv.put(kv.key(), kv);
            }
        } else if (!queryingKeys.isEmpty()) {
            for (String querykey : queryingKeys) {
                KV kv = orderData.getKV(querykey);
                if (kv != null) {
                    allkv.put(querykey, kv);
                } else {
                    kv = goodData.getKV(querykey);
                    if (kv != null) {
                        allkv.put(querykey, kv);
                    } else {
                        kv = buyerData.getKV(querykey);
                        if (kv != null) {
                            allkv.put(querykey, kv);
                        }
                    }
                }
            }
        }
        return new ResultImpl(orderid, allkv);
    }

    public OrderSystem.KeyValue get(String key) {
        return this.kvMap.get(key);
    }

    public OrderSystem.KeyValue[] getAll() {
        return kvMap.values().toArray(new OrderSystem.KeyValue[0]);
    }

    public long orderId() {
        return orderid;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("orderid: " + orderid + " {");
        if (kvMap != null && !kvMap.isEmpty()) {
            for (KV kv : kvMap.values()) {
                sb.append(kv.toString());
                sb.append(",\n");
            }
        }
        sb.append('}');
        return sb.toString();
    }
}
