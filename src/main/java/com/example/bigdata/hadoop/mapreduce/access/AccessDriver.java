package com.example.bigdata.hadoop.mapreduce.access;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * MapReduce 实现统计用户上、下行流量统计
 */
public class AccessDriver {

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME","hadoop");
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://hadoop001:8020");
        // 创建job实例
        Job job = Job.getInstance(configuration);
        job.setJarByClass(AccessDriver.class);
        job.setMapperClass(AccessMapper.class);
        job.setReducerClass(AccessReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Access.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Access.class);
        // 设置自定义的partition类
        job.setPartitionerClass(AccessPartitioner.class);
        // 设置分区数
        job.setNumReduceTasks(2);

        String input = "/access/input";
        String output = "/access/output";
        Path inputPath = new Path(input);
        Path outputPath = new Path(output);
        FileSystem fs = FileSystem.get(new URI(""), configuration, "hadoop");
        if (fs.exists(outputPath)) {
            fs.delete(outputPath,true);
        }
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        boolean executeResult = job.waitForCompletion(true);

        System.exit(executeResult ? 0 : -1);
    }
}
