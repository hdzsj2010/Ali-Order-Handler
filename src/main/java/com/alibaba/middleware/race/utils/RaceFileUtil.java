package com.alibaba.middleware.race.utils;

/**
 * Created by wa on 2016/7/22.
 */
public class RaceFileUtil {
    public final static String BUYER_PREFIX = "buyer_";
    public final static String GOOD_PREFIX = "good_";
    public final static String ORDER_PREFIX = "order";

    public final static String NEW_FILE_SUFFIX = "_new";
    public final static int STORE_FILE_NUM = 1024;
    public static char LINUX_LF = '\r';

    public static int BUYER_GOOD_FILE_NUM = 128;
    public static String BUYER_INDEX_PREFIX = "buyer_index_";
    public static String GOOD_INDEX_PREFIX = "good_index_";
    public static String SEMICOLON = ";";
    public static String COMMA = ",";

    public static int GOOD_LINE_NUM = 1300000;
    public static int BUYER_LINE_NUM = 700000;
    public static int RUNTIME_LIMIT = 3570000;
//    public static int GOOD_LINE_NUM = 400;
//    public static int BUYER_LINE_NUM = 200;
//    public static int RUNTIME_LIMIT = 5000;

    public static int getHashCode(String s) {
        return (s.hashCode() & 0x7FFFFFFF);
    }
}
