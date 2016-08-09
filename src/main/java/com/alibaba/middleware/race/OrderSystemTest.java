package com.alibaba.middleware.race;

import java.io.IOException;
import java.util.*;


public class OrderSystemTest {
    private HashSet<String> createQueryKeys(Collection<String> keys) {
        if (keys == null) {
            return null;
        }
        return new HashSet<String>(keys);
    }

    public void testDemo(Collection<String> keys) {
        Set<String> queryingKeys = createQueryKeys(keys);
        if (keys == null) {
            System.out.println("is null");
        } else if (keys.isEmpty()) {
            System.out.println("is empty");
        } else {
            System.out.println("have keys");
        }

    }

    public static void main(String[] args) {
        OrderSystemTest ost = new OrderSystemTest();
        ost.testDemo(null);
        ost.testDemo(new ArrayList<String>());
        List<String> queryingKeys = new ArrayList<String>();
        queryingKeys.add("contactphone");
        queryingKeys.add("buyerid");
        queryingKeys.add("good_name");
        ost.testDemo(queryingKeys);
//        long oldTime = System.currentTimeMillis();
//        OrderSystem orderSystem=new OrderSystemImpl();
//        HashSet<String> goodFiles = new HashSet<String>();
//        HashSet<String> buyerFiles = new HashSet<String>() ;
//        HashSet<String> orderFiles = new HashSet<String>();
//        HashSet<String> storeFiles = new HashSet<String>();
//        goodFiles.add("good_records.txt");
//        buyerFiles.add("buyer_records.txt");
//        orderFiles.add("order_records.txt");
//        try {
//            orderSystem.construct(orderFiles,buyerFiles,goodFiles,storeFiles);
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        long newTime=System.currentTimeMillis();
//        System.out.println("haoshi:"+(newTime-oldTime));
    }
}
