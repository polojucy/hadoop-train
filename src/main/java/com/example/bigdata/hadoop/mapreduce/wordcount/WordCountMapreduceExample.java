package com.example.bigdata.hadoop.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * MapReduce 实现（wordcount）词频统计
 */
public class WordCountMapreduceExample {

    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME","hadoop");

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://hadoop001:8020");

        // 创建job实例
        Job job = Job.getInstance(configuration);
        // 设置入口类
        job.setJarByClass(WordCountMapreduceExample.class);
        // 设置自定义mapper|reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        // 设置mapper的输出类型<K,V>
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //  设置reducer的输出类型<K,V>
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        // 设置combiner类优化map阶段
        job.setCombinerClass(WordCountReducer.class);

        String input = "/wordcount/input";
        String output = "/wordcount/output";
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
