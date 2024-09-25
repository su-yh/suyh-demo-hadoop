package com.suyh.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

/**
 * @author suyh
 * @since 2024-05-25
 */
public class HdfsClient {
    public static void main(String[] args) throws Exception {
        {
            // 从hdfs 文件系统中读文件
            URI uri = new URI("hdfs://192.168.8.58:8020");
            String filePath = "/temp/tmp.txt";
            readText(uri, filePath);
        }

        {
            // 从本地文件系统中读文件
            URI uri = new URI("file:///");
            String filePath = "/temp/tmp.txt";
            readText(uri, filePath);
        }

    }

    private static void readText(URI uri, String filePath) throws Exception {
        Configuration configuration = new Configuration();

        FileSystem fs = FileSystem.get(uri, configuration);
//        FSDataInputStream inputStream = fs.open(new Path("hdfs://hadoop102:8020/tmp.txt"));
        FSDataInputStream inputStream = fs.open(new Path(filePath));

        FsStatus status = fs.getStatus();
        System.out.println("ststus: " + status.getCapacity());

        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String line;
        while ((line = reader.readLine())!= null) {
            // 在这里可以对每一行进行解析处理
            System.out.println(line);
        }
        reader.close();
        inputStream.close();
        fs.close();
    }
}
