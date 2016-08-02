package com.alibaba.middleware.race;

import com.alibaba.middleware.race.entity.KV;
import com.alibaba.middleware.race.entity.ResultImpl;
import com.alibaba.middleware.race.entity.Row;
import com.alibaba.middleware.race.utils.RaceFileUtil;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by lby on 2016/7/12.
 */
public class OrderSystemImpl implements OrderSystem {
    public ConcurrentHashMap<Long, Row> hotOrder;
    //    public ConcurrentHashMap<String, Row> hotGood;
//    public ConcurrentHashMap<String, Row> hotBuyer;
    public ConcurrentHashMap<String, String> cacheGood;
    public ConcurrentHashMap<String, String> cacheBuyer;

    //保存是否排序完的结果,key为good索引文件的文件名
//    private ConcurrentHashMap<String, Future<String>> sortResult;
    //存放good和buyer的最后若干行
//    public volatile MaxSizeHashMap<String, String> goodCache;
//    public volatile MaxSizeHashMap<String, String> buyerCache;

    private List<FileWriter> orderWriters;
    private List<FileWriter> goodWriters;       //order里goodid的索引
    private List<FileWriter> buyerWriters;      //order里buyerid的索引

    private FileWriter[] goodIndexWriters;  //good的索引
    private FileWriter[] buyerIndexWriters; //buyer的索引
    private List<String> storeFolderList;

    public OrderSystemImpl() {
        hotOrder = new ConcurrentHashMap<Long, Row>();
//        hotGood = new ConcurrentHashMap<String, Row>();
//        hotBuyer = new ConcurrentHashMap<String, Row>();
        cacheGood = new ConcurrentHashMap<String, String>();
        cacheBuyer = new ConcurrentHashMap<String, String>();

        orderWriters = new ArrayList<FileWriter>(RaceFileUtil.STORE_FILE_NUM);
        goodWriters = new ArrayList<FileWriter>(RaceFileUtil.STORE_FILE_NUM);
        buyerWriters = new ArrayList<FileWriter>(RaceFileUtil.STORE_FILE_NUM);

        goodIndexWriters = new FileWriter[RaceFileUtil.BUYER_GOOD_FILE_NUM];
        buyerIndexWriters = new FileWriter[RaceFileUtil.BUYER_GOOD_FILE_NUM];
        storeFolderList = new ArrayList<String>();
//        sortResult = new ConcurrentHashMap<String, Future<String>>(512);

    }

    //*******************************************构建部分***********************************************************//
    //构建订单表的索引
    class ConstrutOrderIndex implements Runnable {
        private String orderFileName;

        public ConstrutOrderIndex(String fileName) {
            this.orderFileName = fileName;
        }

        @Override
        public void run() {
            long startTime = System.currentTimeMillis();
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new FileReader(orderFileName));
                String s;
                while ((s = reader.readLine()) != null) {
                    String[] fields = s.split("\t");
                    String orderId = null;
                    String buyerId = null;
                    String goodId = null;
                    for (String field : fields) {
                        int p = field.indexOf(":");
                        String key = field.substring(0, p);
                        String value = field.substring(p + 1);
                        if (orderId == null || buyerId == null || goodId == null) {
                            if (key.equals("orderid")) {
                                orderId = value;
                            }
                            if (key.equals("buyerid")) {
                                buyerId = value;
                            }
                            if (key.equals("goodid")) {
                                goodId = value;
                            }
                        }
                    }
                    /*long orderId = Long.parseLong(fields[0].substring(8));
                    String buyerId = fields[2].substring(8);
                    String goodId = fields[3].substring(7);*/
                    
                    //计算orderId对256取模的值，再根据这个值从orderWriters中取出对应的FileWriter进行写文件
                    int orderFileIndex = (int) (Long.parseLong(orderId) % RaceFileUtil.STORE_FILE_NUM);
                    //orderkey的存储格式为：orderId;{readline数据}\r
                    String orderkey = orderId.concat(RaceFileUtil.SEMICOLON).concat(s) + RaceFileUtil.LINUX_LF;
                    orderWriters.get(orderFileIndex).write(orderkey);

                    //同上，只不过取模的时候根据Id的再哈希值进行取
                    int goodFileIndex = RaceFileUtil.getHashCode(goodId) % RaceFileUtil.STORE_FILE_NUM;
                    String goodKey = goodId.concat(RaceFileUtil.SEMICOLON).concat(s) + RaceFileUtil.LINUX_LF;
                    goodWriters.get(goodFileIndex).write(goodKey);

                    int buyerFileIndex = RaceFileUtil.getHashCode(buyerId) % RaceFileUtil.STORE_FILE_NUM;
                    String buyerKey = buyerId.concat(RaceFileUtil.SEMICOLON).concat(s) + RaceFileUtil.LINUX_LF;
                    buyerWriters.get(buyerFileIndex).write(buyerKey);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                assert reader != null;
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            long endTime = System.currentTimeMillis();
            System.out.println(orderFileName + "has done :" + (endTime - startTime));
        }
    }

    //构建商品表的索引
    class ConstructGoodIndex implements Runnable {
        private String fileName;

        public ConstructGoodIndex(String fileName) {
            this.fileName = fileName;
        }

        @Override
        public void run() {
            try {
                BufferedReader reader = new BufferedReader(new FileReader(fileName));
                String s = null;
                int lineNo = 0;
                while ((s = reader.readLine()) != null) {
                    lineNo++;
                    String[] fields = s.split("\t");
                    String goodid = null;
                    for (String field : fields) {
                        int p = field.indexOf(":");
                        String key = field.substring(0, p);
                        String value = field.substring(p + 1);
                        if (key.equals("goodid")) {
                            goodid = value;
                            break;
                        }
                    }
                    //如果商品记录数超过了指定值（1300000），就将后面的商品记录存放到cacheGood这个ConcurrentHashMap
                    if (lineNo > RaceFileUtil.GOOD_LINE_NUM)
                        cacheGood.put(goodid, s);
                    //和前面的处理交易记录一样
                    int goodFileindex = RaceFileUtil.getHashCode(goodid) % RaceFileUtil.BUYER_GOOD_FILE_NUM;
                    String goodKey = goodid + RaceFileUtil.SEMICOLON + s + RaceFileUtil.LINUX_LF;
                    goodIndexWriters[goodFileindex].write(goodKey);

                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    class ConstructBuyerIndex implements Runnable {
        private String fileName;

        public ConstructBuyerIndex(String fileName) {
            this.fileName = fileName;
        }

        @Override
        public void run() {
            try {
                BufferedReader reader = new BufferedReader(new FileReader(fileName));
                String s = null;
                int lineNo = 0;
                while ((s = reader.readLine()) != null) {
                    lineNo++;
                    String[] fields = s.split("\t");
                    String buyerid = null;
                    for (String field : fields) {
                        int p = field.indexOf(":");
                        String key = field.substring(0, p);
                        String value = field.substring(p + 1);
                        if (key.equals("buyerid")) {
                            buyerid = value;
                            break;
                        }
                    }
                    //如果买家记录数超过了指定值（700000），就将后面的商品记录存放到cacheGood这个ConcurrentHashMap
                    if (lineNo > RaceFileUtil.BUYER_LINE_NUM)
                        cacheBuyer.put(buyerid, s);
                    //和前面的处理交易记录一样
                    int buyerFileIndex = RaceFileUtil.getHashCode(buyerid) % RaceFileUtil.BUYER_GOOD_FILE_NUM;
                    String buyerKey = buyerid + RaceFileUtil.SEMICOLON + s + RaceFileUtil.LINUX_LF;
                    buyerIndexWriters[buyerFileIndex].write(buyerKey);

                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //获取对应的ID所在具体文件路径
    public String decideGoodFile(String goodId) {
        int fileindex = RaceFileUtil.getHashCode(goodId) % RaceFileUtil.STORE_FILE_NUM;//1-255的ID hash对256取模值
        String filePrefix = storeFolderList.get(fileindex % storeFolderList.size());//1-255对storeFolderList大小取模值，用于取前缀,即将这些文件放在不同的目录下面
        return filePrefix + RaceFileUtil.GOOD_PREFIX + fileindex;//返回路径，类似D:\\index\\1\\good_255
    }

    public String decideBuyerFile(String buyerId) {
        int fileindex = RaceFileUtil.getHashCode(buyerId) % RaceFileUtil.STORE_FILE_NUM;
        String filePrefix = storeFolderList.get(fileindex % storeFolderList.size());
        return filePrefix + RaceFileUtil.BUYER_PREFIX + fileindex;//返回路径，类似D:\\index\\1\\buyer_255
    }

    public String decideOrderFile(long orderId) {
        int fileindex = (int) (orderId % RaceFileUtil.STORE_FILE_NUM);
        String filePrefix = storeFolderList.get(fileindex % storeFolderList.size());
        return filePrefix + RaceFileUtil.ORDER_PREFIX + fileindex;//返回路径，类似D:\\index\\1\\order_255
    }

    public String getGoodIndexFile(String goodId) {
        int fileindex = RaceFileUtil.getHashCode(goodId) % RaceFileUtil.BUYER_GOOD_FILE_NUM;
        String filePrefix = storeFolderList.get(fileindex % storeFolderList.size());
        return filePrefix + RaceFileUtil.GOOD_INDEX_PREFIX + fileindex;
    }

    public String getBuyerIndexFile(String buyerId) {
        int fileindex = RaceFileUtil.getHashCode(buyerId) % RaceFileUtil.BUYER_GOOD_FILE_NUM;
        String filePrefix = storeFolderList.get(fileindex % storeFolderList.size());
        return filePrefix + RaceFileUtil.BUYER_INDEX_PREFIX + fileindex;
    }

    
    //初始化！！！
    public void construct(Collection<String> orderFiles,
                          Collection<String> buyerFiles, Collection<String> goodFiles,
                          Collection<String> storeFolders) throws IOException, InterruptedException {

        long startTime = System.currentTimeMillis();
//        System.out.println("buyerFiles size:" + buyerFiles.size());
//        System.out.println("goodFiles size:" + goodFiles.size());
        storeFolderList.addAll(storeFolders);
        int folderSize = storeFolders.size();
        System.out.println("orderfile:" + orderFiles.size());
        //根据不同的文件名生成256个FileWriter，存放到orderWriters、goodWriters、buyerWriters供订单记录构建调用
        for (int i = 0; i < RaceFileUtil.STORE_FILE_NUM; i++) {
            int index = i % folderSize;
            try {
                FileWriter orderWriter = new FileWriter(new File(storeFolderList.get(index) + RaceFileUtil.ORDER_PREFIX + i));
                orderWriters.add(orderWriter);
                FileWriter goodWriter = new FileWriter(new File(storeFolderList.get(index) + RaceFileUtil.GOOD_PREFIX + i));
                goodWriters.add(goodWriter);
                FileWriter buyerWriter = new FileWriter(new File(storeFolderList.get(index) + RaceFileUtil.BUYER_PREFIX + i));
                buyerWriters.add(buyerWriter);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        //根据不同的文件名生成64个FileWriter，存放到orderWriters、goodWriters、buyerWriters供商品记录和买家记录构建调用
        for (int j = 0; j < RaceFileUtil.BUYER_GOOD_FILE_NUM; j++) {
            int index = j % folderSize;
            try {
                FileWriter goodIndexWriter = new FileWriter(new File(storeFolderList.get(index) + RaceFileUtil.GOOD_INDEX_PREFIX + j));
                goodIndexWriters[j] = goodIndexWriter;
                FileWriter buyerIndexWriter = new FileWriter(new File(storeFolderList.get(index) + RaceFileUtil.BUYER_INDEX_PREFIX + j));
                buyerIndexWriters[j] = buyerIndexWriter;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        //开启5个线程为goodfile和buyerfile构建索引，每个线程读一个文件
        ExecutorService indexService = Executors.newFixedThreadPool(6);
        for (String file : goodFiles) {
            indexService.execute(new ConstructGoodIndex(file));
        }
        for (String file : buyerFiles) {
            indexService.execute(new ConstructBuyerIndex(file));
        }

        //开启4个线程为orderfile构建索引，每个线程读一个文件
        ExecutorService executorService = Executors.newFixedThreadPool(6);
        for (String file : orderFiles) {
            executorService.execute(new ConstrutOrderIndex(file));
        }
        //关闭线程池，并且等待工作中的线程完成任务或者超时停止
        executorService.shutdown();
        indexService.shutdown();
        executorService.awaitTermination(59, TimeUnit.MINUTES);
        indexService.awaitTermination(59, TimeUnit.MINUTES);
//        关闭文件流
        for (int j = 0; j < RaceFileUtil.STORE_FILE_NUM; j++) {
            orderWriters.get(j).flush();
            goodWriters.get(j).flush();
            buyerWriters.get(j).flush();
        }

        for (int i = 0; i < RaceFileUtil.BUYER_GOOD_FILE_NUM; i++) {
            buyerIndexWriters[i].flush();
            goodIndexWriters[i].flush();
        }

//        executorService = Executors.newFixedThreadPool(3);
//        for (int i = 0; i < RaceFileUtil.STORE_FILE_NUM; i++) {
//            int index = i % folderSize;
//            String fileName = storeFolderList.get(index) + RaceFileUtil.GOOD_PREFIX + i;
//            Future<String> future = executorService.submit(new ClassifyFile(fileName));
//            sortResult.put(fileName, future);
//            //String buyerFileName= storeFolderList.get(index) + RaceFileUtil.BUYER_PREFIX + i
//            //executorService.submit(new ClassifyFile(storeFolderList.get(index) + RaceFileUtil.GOOD_PREFIX + i));        //为good做排序
//            //executorService.execute(new ClassifyFile(storeFolderList.get(index) + RaceFileUtil.BUYER_PREFIX + i));    //为buyer排序
//        }
        //executorService.shutdown();
        long endTime = System.currentTimeMillis();
//        long leaveTime = 3570000 - (endTime - startTime);
        long leaveTime = RaceFileUtil.RUNTIME_LIMIT - (endTime - startTime);
        System.out.println("leaveTime:" + leaveTime);
//        if (leaveTime > 100)
//            Thread.sleep(leaveTime);
        //executorService.awaitTermination(13, TimeUnit.MINUTES);

        //开启8个线程对文件排序
        /*long startTime=System.currentTimeMillis();
        executorService=Executors.newFixedThreadPool(3);
        for (int j = 0; j < RaceFileUtil.STORE_FILE_NUM; j++) {
            int index = j % folderSize;
            //executorService.execute(new SortFile(storeFolderList.get(index) + RaceFileUtil.BUYER_PREFIX + j));
            executorService.execute(new SortFile(storeFolderList.get(index) + RaceFileUtil.GOOD_PREFIX + j));
        }
        executorService.shutdown();
        executorService.awaitTermination(15,TimeUnit.MINUTES);
        long endTime=System.currentTimeMillis();
        System.out.println("sortTime:"+(endTime-startTime));*/
    }


    //**********************************************从hash文件中查询数据************************************************/
    //根据需要查询的ID，从Hash文件中查询所对应的该行数据，
    // 适用于查找good的Hash文件，buyer的Hash文件以及以orderid作为索引构建的order的Hash文件，该hash文件未排序
    public String getDataByIdFromHashFile(String fileName, String searchId) throws IOException {
        BufferedReader bfr = createReader(fileName);
        try {
            String line = bfr.readLine();
            while (line != null) {
                int p = line.indexOf(";");
                String key = line.substring(0, p);
                String data = line.substring(p + 1);
                if (searchId.equals(key))
                    return data;
                line = bfr.readLine();
            }
        } finally {
            bfr.close();
        }
        return null;
    }

    //根据id查找hash文件中出现的所有键为key，值为searchid的记录，返回所有的记录
    //使用于以buyerid为索引的order的hash文件以及以goodid为索引的order的hash文件，该hash文件未排序
    public ArrayList<String> getALLDataByIdFromHashFile(String fileName, String searchId) throws IOException {
        ArrayList<String> indexData = new ArrayList<String>();
        BufferedReader bfr = createReader(fileName);
        try {
            String line = bfr.readLine();
            while (line != null) {
                int p = line.indexOf(";");
                String key = line.substring(0, p);
                String data = line.substring(p + 1);
                if (key.equals(searchId)) {
                    indexData.add(data);
                }
                line = bfr.readLine();
            }
        } finally {
            bfr.close();
        }
        return indexData;
    }

    //根据id查找hash文件中出现的所有键为key，值为searchid的记录，返回所有的记录
    //使用于以buyerid为索引的order的hash文件以及以goodid为索引的order的hash文件，该hash文件已排序
    public String[] getALLDataByIdFromSortedHashFile(String fileName, String searchId) throws IOException {
        String[] dataList = null;
        BufferedReader bfr = createReader(fileName);
        try {
            String line = bfr.readLine();
            while (line != null) {
//                String[] fields = line.split("\t");
//                for (String field : fields) {
//                    int p = field.indexOf(":");
//                    String filekey = field.substring(0, p);
//                    String value = field.substring(p + 1);
//                    if (filekey.equals(key)) {
//                        if (value.equals(searchId)) {
//                            indexData.add(line);
//                        } else if (!indexData.isEmpty()) {
//                            return indexData;
//                        } else {
//                            break;
//                        }
//                    }
                int p = line.indexOf(";");
                String key = line.substring(0, p);
                String dataLine = line.substring(p + 1);
                if (searchId.equals(key)) {
                    dataList = dataLine.split(",");//文件内容排序的时候，将相同ID的data放在同一行用逗号隔开
                    return dataList;
                }
                line = bfr.readLine();
            }
        } finally {
            bfr.close();
        }
        return dataList;
    }

    //*********************************************查询工具函数部分*****************************************************//
    private BufferedReader createReader(String file) throws FileNotFoundException {
        return new BufferedReader(new FileReader(file));
    }

    private Row createKVMapFromLine(String line) {
        String[] kvs = line.split("\t");
        Row kvMap = new Row();
        for (String rawkv : kvs) {
            int p = rawkv.indexOf(':');
            String key = rawkv.substring(0, p);
            String value = rawkv.substring(p + 1);
            if (key.length() == 0 || value.length() == 0) {
                throw new RuntimeException("Bad data:" + line);
            }
            KV kv = new KV(key, value);
            kvMap.put(kv.key(), kv);
        }
        return kvMap;
    }

    //解析出order中的goodId和buyerId，并从good和buyer的文件中找出对应的信息，通过createResultRow将keys对应的信息统一封装到ResultImpl中（即Row的hashmap）
    private ResultImpl createResultFromOrderData(Row orderData,
                                                 Collection<String> keys) {
        String goodId = orderData.getKV("goodid").valueAsString();
        String goodDataStr = cacheGood.get(goodId);
        if (goodDataStr == null) {
            String fileName = getGoodIndexFile(goodId);
            try {
                goodDataStr = getDataByIdFromHashFile(fileName, goodId);
                cacheGood.put(goodId, goodDataStr);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        Row goodData = createKVMapFromLine(goodDataStr);

        String buyerId = orderData.getKV("buyerid").valueAsString();
        String buyerDataStr = cacheBuyer.get(buyerId);
        if (buyerDataStr == null) {
            String fileName = getBuyerIndexFile(buyerId);
            try {
                buyerDataStr = getDataByIdFromHashFile(fileName, buyerId);
                cacheBuyer.put(buyerId, buyerDataStr);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        Row buyerData = createKVMapFromLine(buyerDataStr);
        
        try {
            hotOrder.put(orderData.getKV("orderid").valueAsLong(), orderData);
        } catch (TypeException e) {
            e.printStackTrace();
        }
        
        return ResultImpl.createResultRow(orderData, buyerData, goodData,
                createQueryKeys(keys));
    }

    private HashSet<String> createQueryKeys(Collection<String> keys) {
        if (keys == null) {
            return null;
        }
        return new HashSet<String>(keys);
    }


//**********************************************查询实现接口部分******************************************************//

    //“索引文件”存的是原始数据
    public Result queryOrder(long orderId, Collection<String> keys) {
        Row orderData = null;
        //如果hotOrder中已经包含说明这个order的全部数据已经封装并保存到cache中了
        if (hotOrder.containsKey(orderId)) {
            orderData = hotOrder.get(orderId);
//            Row goodData = hotGood.get(orderData.getKV("goodid").valueAsString());
//            Row buyerData = hotBuyer.get(orderData.getKV("buyerid").valueAsString());
            Row goodData = createKVMapFromLine(cacheGood.get(orderData.getKV("goodid").valueAsString()));
            Row buyerData = createKVMapFromLine(cacheBuyer.get(orderData.getKV("buyerid").valueAsString()));
            return ResultImpl.createResultRow(orderData, buyerData, goodData,
                    createQueryKeys(keys));
        } else {
            String fileName = decideOrderFile(orderId);
            try {
                String data = getDataByIdFromHashFile(fileName, Long.toString(orderId));
                //System.out.println("data is:"+data);
                if (data != null)
                    orderData = createKVMapFromLine(data);
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (orderData != null)
                return createResultFromOrderData(orderData, createQueryKeys(keys));
            else
                return null;
        }
    }

    public Iterator<Result> queryOrdersByBuyer(long startTime, long endTime,
                                               String buyerid) {
        TreeMap<Long, Row> orderDataSortedByBuyerCreateTime = new TreeMap<Long, Row>();
        String fileName = decideBuyerFile(buyerid);
//        Future<Boolean> future=sortResult.get(fileName);
//        if(future.isDone()){
//            System.out.println("yes,hassort");
//            try {
//                String[] indexData = getALLDataByIdFromSortedHashFile(fileName, buyerid);
//                for (String data : indexData) {
//                    Row orderData = createKVMapFromLine(data);
//                    orderDataSortedByBuyerCreateTime.put(orderData.getKV("createtime").valueAsLong(), orderData);
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//            } catch (TypeException e) {
//                e.printStackTrace();
//            }
//        }else {
//            System.out.println("not");
//            try {
//                ArrayList<String> indexData = getALLDataByIdFromHashFile(fileName, buyerid);
//                for (String data : indexData) {
//                    Row orderData = createKVMapFromLine(data);
//                    orderDataSortedByBuyerCreateTime.put(orderData.getKV("createtime").valueAsLong(), orderData);
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//            } catch (TypeException e) {
//                e.printStackTrace();
//            }
//        }
        try {
            ArrayList<String> indexData = getALLDataByIdFromHashFile(fileName, buyerid);
            for (String data : indexData) {
                Row orderData = createKVMapFromLine(data);
                orderDataSortedByBuyerCreateTime.put(orderData.getKV("createtime").valueAsLong(), orderData);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TypeException e) {
            e.printStackTrace();
        }
        final SortedMap<Long, Row> orders = orderDataSortedByBuyerCreateTime.subMap(startTime, endTime);

        return new Iterator<Result>() {

            SortedMap<Long, Row> o = orders;

            public boolean hasNext() {
                return o != null && o.size() > 0;
            }

            public Result next() {
                if (!hasNext()) {
                    return null;
                }
                Long lastKey = o.lastKey();
                Row orderData = o.get(lastKey);
                o.remove(lastKey);
                return createResultFromOrderData(orderData, null);
            }

            public void remove() {

            }
        };
    }

    public Iterator<Result> queryOrdersBySaler(String salerid, String goodid,
                                               Collection<String> keys) {
//        Iterator iter = sortResult.entrySet().iterator();
//        while (iter.hasNext()) {
//            Map.Entry entry = (Map.Entry) iter.next();
//            String key = entry.getKey().toString();
//            Future<Boolean> future = sortResult.get(key);
//            System.out.println(key + ":" + future.isDone());
//        }
        final Collection<String> queryKeys = keys;
        String fileName = decideGoodFile(goodid);
        TreeMap<Long, Row> orderDataSortedByBGoodOrderId = new TreeMap<Long, Row>();
//        System.out.println("QUERYORDER:" + fileName);
//        Future<String> future = sortResult.get(fileName);
//        if (future.isDone()) {
//            System.out.println("queryOrdersBySaler:isdone");
//            String newFileName = null;
//            try {
//                newFileName = future.get(1, TimeUnit.MINUTES);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            } catch (ExecutionException e) {
//                e.printStackTrace();
//            } catch (TimeoutException e) {
//                e.printStackTrace();
//            }
//            if (newFileName != null)
//                fileName = newFileName;
//            try {
//                String[] indexData = getALLDataByIdFromSortedHashFile(fileName, goodid);
//                for (String data : indexData) {
//                    Row orderData = createKVMapFromLine(data);
//                    orderDataSortedByBGoodOrderId.put(orderData.getKV("orderid").valueAsLong(), orderData);
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//            } catch (TypeException e) {
//                e.printStackTrace();
//            }
//        } else {
//            System.out.println("queryOrdersBySaler:notdone");
        try {
            ArrayList<String> indexData = getALLDataByIdFromHashFile(fileName, goodid);
            for (String data : indexData) {
                Row orderData = createKVMapFromLine(data);
                orderDataSortedByBGoodOrderId.put(orderData.getKV("orderid").valueAsLong(), orderData);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TypeException e) {
            e.printStackTrace();
        }
//        }
        
        //注意内部类访问的局部变量要设为final
        final SortedMap<Long, Row> orders = orderDataSortedByBGoodOrderId;
        return new Iterator<Result>() {
            SortedMap<Long, Row> o = orders;

            @Override
            public boolean hasNext() {
                return o != null && o.size() > 0;
            }

            @Override
            public Result next() {
                if (!hasNext()) {
                    return null;
                }
                Long firstKey = o.firstKey();
                Row orderData = o.get(firstKey);
                o.remove(firstKey);
                return createResultFromOrderData(orderData, createQueryKeys(queryKeys));
            }

            @Override
            public void remove() {

            }
        };
    }

    public KeyValue sumOrdersByGood(String goodid, String key) {
        String fileName = decideGoodFile(goodid);
        TreeMap<Long, Row> ordersData = new TreeMap<Long, Row>();
//        Future<String> future = sortResult.get(fileName);
//        if (future.isDone()) {
//            System.out.println("sumOrdersByGood:isdone");
//            String newFileName = null;
//            try {
//                newFileName = future.get(1, TimeUnit.MINUTES);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            } catch (ExecutionException e) {
//                e.printStackTrace();
//            } catch (TimeoutException e) {
//                e.printStackTrace();
//            }
//            if (newFileName != null)
//                fileName = newFileName;
//            try {
//                String[] indexData = getALLDataByIdFromSortedHashFile(fileName, goodid);
//                for (String data : indexData) {
//                    Row orderData = createKVMapFromLine(data);
//                    ordersData.put(orderData.getKV("orderid").valueAsLong(), orderData);
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//            } catch (TypeException e) {
//                e.printStackTrace();
//            }
//        } else {
//            System.out.println("sumOrdersByGood:notdone");
        try {
            ArrayList<String> indexData = getALLDataByIdFromHashFile(fileName, goodid);
            for (String data : indexData) {
                Row orderData = createKVMapFromLine(data);
                ordersData.put(orderData.getKV("orderid").valueAsLong(), orderData);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TypeException e) {
            e.printStackTrace();
        }
//        }

        if (ordersData.isEmpty()) {
            return null;
        }
        HashSet<String> queryingKeys = new HashSet<String>();
        queryingKeys.add(key);
        List<ResultImpl> allData = new ArrayList<ResultImpl>(ordersData.size());
        for (Row orderData : ordersData.values()) {
            allData.add(createResultFromOrderData(orderData, queryingKeys));
        }

        // accumulate as Long
        try {
            boolean hasValidData = false;
            long sum = 0;
            for (ResultImpl r : allData) {
                KeyValue kv = r.get(key);
                if (kv != null) {
                    sum += kv.valueAsLong();
                    hasValidData = true;
                }
            }
            if (hasValidData) {
                return new KV(key, Long.toString(sum));
            }
        } catch (TypeException e) {
        }

        // accumulate as double
        try {
            boolean hasValidData = false;
            double sum = 0;
            for (ResultImpl r : allData) {
                KeyValue kv = r.get(key);
                if (kv != null) {
                    sum += kv.valueAsDouble();
                    hasValidData = true;
                }
            }
            if (hasValidData) {
                return new KV(key, Double.toString(sum));
            }
        } catch (TypeException e) {
        }

        return null;
    }

    public static void main(String[] args) throws IOException,
            InterruptedException {

        // init order system
        List<String> orderFiles = new ArrayList<String>();
        List<String> buyerFiles = new ArrayList<String>();
        List<String> goodFiles = new ArrayList<String>();
        List<String> storeFolders = new ArrayList<String>();
        orderFiles.add("order.0.0");
        orderFiles.add("order.0.3");
        orderFiles.add("order.1.1");
        orderFiles.add("order.2.2");
        buyerFiles.add("buyer.0.0");
        buyerFiles.add("buyer.1.1");
        goodFiles.add("good.0.0");
        goodFiles.add("good.1.1");
        goodFiles.add("good.2.2");
        storeFolders.add("D:\\index\\1\\");
        storeFolders.add("D:\\index\\2\\");
        storeFolders.add("D:\\index\\3\\");
        long startRunTime = System.currentTimeMillis();
        System.out.println("开始构建：" + System.currentTimeMillis());
        OrderSystem os = new OrderSystemImpl();
        os.construct(orderFiles, buyerFiles, goodFiles, storeFolders);
        System.out.println("构建完成：" + System.currentTimeMillis());
        long endRunTime = System.currentTimeMillis();
        System.out.println("构建时间：" + (endRunTime - startRunTime));
        startRunTime = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            //测试queryOrder
//        long orderid = 590372877;
            long orderid = 609670049;
            System.out.println("\n查询订单号为" + orderid + "的订单");
            System.out.println(os.queryOrder(orderid, null));
            System.out.println("\n查询订单号为" + orderid + "的订单，查询的keys为空，返回订单，但没有kv数据");
            System.out.println(os.queryOrder(orderid, new ArrayList<String>()));
            System.out.println("\n查询订单号为" + orderid
                    + "的订单的contactphone, buyerid, good_name字段");
            List<String> queryingKeys = new ArrayList<String>();
//        queryingKeys.add("contactphone");
//        queryingKeys.add("buyerid");
//        queryingKeys.add("good_name");
            queryingKeys.add("description");
            Result result = os.queryOrder(orderid, queryingKeys);
            System.out.println(result);
            System.out.println("\n查询订单号不存在的订单");
            result = os.queryOrder(1111, queryingKeys);
            if (result == null) {
                System.out.println(1111 + " order not exist");
            }
//
            //测试queryOrdersByBuyer
            String buyerid = "wx-a0e0-6bda77db73ca";
            long startTime = 1462018520;
            long endTime = 1473999229;
            System.out.println("\n查询买家ID为" + buyerid + "的一定时间范围内的订单");
            Iterator<Result> it = os.queryOrdersByBuyer(startTime, endTime, buyerid);
            while (it.hasNext()) {
                System.out.println(it.next());
            }
            //测试queryOrdersBySaler
            String goodid = "gd-b972-6926df8128c3";
            String salerid = "almm-b250-b1880d628b9a";
            ArrayList<String> keys = new ArrayList<String>();
            keys.add("a_o_30709");
            keys.add("a_g_32587");
            System.out.println("\n查询商品id为" + goodid + "，商家id为" + salerid + "的订单");
            Iterator<Result> its = os.queryOrdersBySaler(salerid, goodid, keys);
            while (its.hasNext()) {
                System.out.println(its.next());
            }
            goodid = "al-a63c-e1e294d6bcb1";
            salerid = "tm-bad2-ec455f2bcbc0";
            ArrayList<String> keys1 = new ArrayList<String>();
            keys1.add("buyername");
            System.out.println("\n查询商品id为" + goodid + "，商家id为" + salerid + "的订单");
            its = os.queryOrdersBySaler(salerid, goodid, keys1);
            while (its.hasNext()) {
                System.out.println(its.next());
            }
            //测试sumOrdersByGood
            goodid = "al-9c4c-ac9ed4b6ad35";
//        goodid = "aye-945e-35a09f491917";
            String attr = "offprice";
            System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
            System.out.println(os.sumOrdersByGood(goodid, attr));

            attr = "done";
            System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
            KeyValue sum = os.sumOrdersByGood(goodid, attr);
            if (sum == null) {
                System.out.println("由于该字段是布尔类型，返回值是null");
            }
//
            attr = "foo";
            System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
            sum = os.sumOrdersByGood(goodid, attr);
            if (sum == null) {
                System.out.println("由于该字段不存在，返回值是null");
            }
        }
        endRunTime = System.currentTimeMillis();
        System.out.println("查询时间：" + (endRunTime - startRunTime));

    }
}
