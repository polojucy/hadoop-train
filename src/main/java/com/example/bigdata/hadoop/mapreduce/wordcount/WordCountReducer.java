package com.example.bigdata.hadoop.mapreduce.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 自定义reducer
 * 将map的结果：(hello,1) (world,1) (hello,1)
 * 归并统计为：(hello,2) (world,1)
 */
public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0;
        for (LongWritable value : values) {
            long v = value.get();
            count += v;
        }
        context.write(key, new LongWritable(count));
    }
}
