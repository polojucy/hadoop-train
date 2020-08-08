package com.example.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class WordCountExample {

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        // 设置副本数为1
        conf.set("dfs.replication","1");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop001:8020"),conf,"hadoop");
        Map<String, Integer> wordCountMap = new HashMap<>();

        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path("/wordcount"), false);
        while (iterator.hasNext()) {
            LocatedFileStatus current = iterator.next();
            Path currentPath = current.getPath();
            FSDataInputStream in = fs.open(currentPath);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            reader.lines().forEach(e -> {
                String[] words = e.split(",");
                for (String word : words) {
                    Integer count = wordCountMap.get(word);
                    if (count == null) {
                        wordCountMap.put(word, 1);
                    }else {
                        wordCountMap.put(word, count + 1);
                    } 
                }
            });
            in.close();
        }
        fs.close();

        for (Map.Entry<String,Integer> entry : wordCountMap.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
    }

}
