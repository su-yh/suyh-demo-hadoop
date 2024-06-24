package com.suyh.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * @author suyh
 * @since 2024-05-25
 */
public class HdfsClient {
    public static void main(String[] args) throws Exception {
//        URI uri = new URI("hdfs://hadoop102:8020");
        URI uri = new URI("file:///");
        Configuration configuration = new Configuration();

        FileSystem fs = FileSystem.get(uri, configuration);
//        FSDataInputStream open = fs.open(new Path("hdfs://hadoop102:8020/tmp.txt"));
        FSDataInputStream open = fs.open(new Path("/temp/tmp.txt"));

        FsStatus status = fs.getStatus();
        System.out.println("ststus: " + status.getCapacity());
    }
}
