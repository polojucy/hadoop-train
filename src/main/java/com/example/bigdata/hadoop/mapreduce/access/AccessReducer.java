package com.example.bigdata.hadoop.mapreduce.access;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 自定义Reducer
 * 实现：将Access中的数据求和
 */
public class AccessReducer extends Reducer<Text, Access, NullWritable, Access> {

    @Override
    protected void reduce(Text key, Iterable<Access> accesses, Context context) throws IOException, InterruptedException {
        Double upCount = 0.0;
        Double downCount = 0.0;
        for (Access access : accesses) {
            upCount += access.getUp();
            downCount += access.getDown();
        }

        context.write(NullWritable.get(),
                new Access(key.toString(), upCount, downCount, upCount + downCount));
    }
}
