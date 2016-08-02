package com.alibaba.middleware.race.sortfile;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by wa on 2016/7/25.
 */
public class SortFile implements Runnable {
    private String fileName;

    public SortFile(String fileName) {
        this.fileName = fileName;
    }

    @Override
    public void run() {
        try {
            List<File> files = ExternalSort.sortInBatch(new File(fileName));
            ExternalSort.mergeSortedFiles(files, new File(fileName));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
