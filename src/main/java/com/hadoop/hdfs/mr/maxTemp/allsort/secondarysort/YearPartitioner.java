package com.hadoop.hdfs.mr.maxTemp.allsort.secondarysort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class YearPartitioner extends Partitioner<ComboKey, NullWritable> {

    public int getPartition(ComboKey key, NullWritable value, int i) {
        int year = key.getYear();
        return year % i;
    }
}
