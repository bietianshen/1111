package com.hadoop.hdfs.mr.maxTemp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyTemPPartitioner extends Partitioner<IntWritable, IntWritable> {

    public int getPartition(IntWritable key, IntWritable value, int i) {

        if(key.get()-1970<33){
            return 0;
        }else if(key.get()-1970<66){
            return 1;
        }else{
            return 2;
        }
    }
}
