package com.suyh.hdfs;

import lombok.extern.slf4j.Slf4j;
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
@Slf4j
public class HdfsClient {
    public static void main(String[] args) throws Exception {
        if (true) {
            {
                String filePath = "hdfs://192.168.8.58:8020/temp/tmp.txt";
                readTextPlus(filePath);
            }
            System.out.println("###########################################");
            {
                String filePath = "file:///temp/tmp.txt";
                readTextPlus(filePath);
            }
            System.out.println("=========================================");
            {
                String filePath = "/temp/tmp.txt";
                readTextPlus(filePath);
            }
            return;
        }
        if (false) {
            {
                URI uri = new URI("hdfs://192.168.8.58:8020/temp/tmp.txt");
                String scheme = uri.getScheme();
                String filePath = uri.getPath();
                String pre = String.format("%s://%s", scheme, uri.getAuthority());
                String fmt = String.format("scheme: %s, pr: %s, path: %s", scheme, pre, filePath);
                System.out.println(fmt);
            }
            {
                URI uri = new URI("file:///temp/tmp.txt");
                String scheme = uri.getScheme();
                String filePath = uri.getPath();
                String pre = String.format("%s://%s", scheme, uri.getAuthority());
                String fmt = String.format("scheme: %s, pr: %s, path: %s", scheme, pre, filePath);
                System.out.println(fmt);
            }
            {
                URI uri = new URI("/temp/tmp.txt");
                String scheme = uri.getScheme();
                String filePath = uri.getPath();
                String pre = String.format("%s://%s", scheme, uri.getAuthority());
                String fmt = String.format("scheme: %s, pr: %s, path: %s", scheme, pre, filePath);
                System.out.println(fmt);
            }
            return;
        }
        if (false) {
            {
                Path path = new Path("hdfs://192.168.8.58:8020/temp/tmp.txt");
                URI uri = path.toUri();
                String scheme = uri.getScheme();
                String filePath = uri.getPath();
                String pre = String.format("%s://%s", scheme, uri.getAuthority());
                String fmt = String.format("scheme: %s, pr: %s, path: %s", scheme, pre, filePath);
                System.out.println(fmt);
            }
            {
                Path path = new Path("/temp/tmp.txt");
                URI uri = path.toUri();
                String scheme = uri.getScheme();
                String filePath = uri.getPath();
                String pre = String.format("%s://%s", scheme, uri.getAuthority());
                String fmt = String.format("scheme: %s, pr: %s, path: %s", scheme, pre, filePath);
                System.out.println(fmt);
            }
            return;
        }
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

    /**
     * String filePath = "hdfs://192.168.8.58:8020/temp/tmp.txt";
     * String filePath = "file:///temp/tmp.txt";
     * String filePath = "/temp/tmp.txt";
     */
    private static void readTextPlus(String filePath) throws Exception {
        URI uri = new URI(filePath);
        String scheme = uri.getScheme();

        Configuration configuration = new Configuration();

        FileSystem fs = scheme != null
                ? FileSystem.get(uri, configuration)
                // 如果uri 不传，则默认是 file:/// 协议，读本地文件
                // 但如果修改了默认协议，则这里就使用对应的协议
                // 如： conf.set("fs.defaultFS", "hdfs://192.168.8.58:8020");
                : FileSystem.get(configuration); // 默认是 file:/// 协议
        FSDataInputStream inputStream = fs.open(new Path(uri.getPath()));

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
