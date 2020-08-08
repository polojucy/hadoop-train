package com.example.bigdata.hadoop.mapreduce.access;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * MapReduce 自定义分区规则类
 * 实现根据phone的奇、偶数分别放到不同的分区中
 */
public class AccessPartitioner extends Partitioner<Text, Access> {

    @Override
    public int getPartition(Text text, Access access, int numPartitions) {
        if (Long.parseLong(access.getPhone()) % 2 == 0) {
            return 0;
        }
        return 1;
    }
}
