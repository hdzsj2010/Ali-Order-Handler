package com.alibaba.middleware.race.construct;

import com.alibaba.middleware.race.utils.RaceFileUtil;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Created by wa on 2016/7/27.
 */
public class ClassifyFile implements Callable<String> {
    private String fileName;

    public ClassifyFile(String fileName) {
        this.fileName = fileName;
    }


    @Override
    public String call() throws Exception {
//        long startTime = System.currentTimeMillis();
        Map<String, String> map = new HashMap(1024);
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(fileName));
            String s;
            while ((s = reader.readLine()) != null) {
                //String[] fields = s.split(RaceFileUtil.SEMICOLON);
                int p = s.indexOf(RaceFileUtil.SEMICOLON);

                String goodId = s.substring(0, p);
                String value = map.get(goodId);
                if (value != null) {
                    value = value + RaceFileUtil.COMMA + s.substring(p + 1);
                } else {
                    value = s.substring(p + 1);
                }
                map.put(goodId, value);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        String newFileName = fileName + RaceFileUtil.NEW_FILE_SUFFIX;
        try {
            FileWriter fileWriter = new FileWriter(newFileName);
            for (Map.Entry<String, String> entry : map.entrySet()) {
                fileWriter.write(entry.getKey() + RaceFileUtil.SEMICOLON + entry.getValue() + RaceFileUtil.LINUX_LF);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
//        long endTime = System.currentTimeMillis();
//        System.out.println("sort time" + (endTime - startTime));
        System.out.println(newFileName);
        return newFileName;
    }
}
