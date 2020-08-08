package com.example.bigdata.hadoop.mapreduce.access;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 自定义Mapper
 * 功能: 将文件每行数据使用","分割，并转换为Access对象
 */
public class AccessMapper extends Mapper<LongWritable, Text, Text, Access> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        String phone = split[0];
        Double up = Double.valueOf(split[1]);
        Double down = Double.valueOf(split[2]);
        context.write(new Text(phone), new Access(phone, up, down, up + down));
    }
}
