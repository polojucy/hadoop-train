package com.example.bigdata.hadoop.mapreduce.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *  自定义mapper
 *  将文件中的单词拆开，并且映射次数
 *  hello world hello => (hello,1) (world,1) (hello,1)
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 分割文件
        String[] split = value.toString().split(",");
        for (String str : split) {
            //  (hello,1) (word,1)
            context.write(new Text(str), new LongWritable(1));
        }

    }
}
