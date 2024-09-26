package com.suyh.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.yaml.snakeyaml.Yaml;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

/**
 * @author suyh
 * @since 2024-09-25
 */
public class HdfsClientYaml {
    public static void main(String[] args) {
        String hdfsFilePath = "/temp/application-beta.yaml";
        try {
            Configuration conf = new Configuration();
            URI uri = new URI("hdfs://192.168.8.58:8020");
            FileSystem fs = FileSystem.get(uri, conf);
            Path path = new Path(hdfsFilePath);
            FSDataInputStream inputStream = fs.open(path);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            Yaml yaml = new Yaml();
            // 读取整个 YAML 文件内容到一个字符串
            StringBuilder yamlContent = new StringBuilder();
            String line;
            while ((line = reader.readLine())!= null) {
                yamlContent.append(line).append("\n");
            }
            // 解析 YAML 内容
            Object yamlObject = yaml.load(yamlContent.toString());
            System.out.println("Parsed YAML object: " + yamlObject);
            reader.close();
            inputStream.close();
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
