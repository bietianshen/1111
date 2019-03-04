package com.hadoop.hdfs.mr.maxTemp.allsort;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxTampMapper extends Mapper<IntWritable, IntWritable, IntWritable, IntWritable> {
    protected void map(IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException {

       /* String[] args = value.toString().split(" ");
        System.out.println(value);
        IntWritable outKey = new IntWritable(Integer.valueOf(args[0]));
        IntWritable outValue = new IntWritable(Integer.valueOf(args[1]));*/
        context.write(key,value);

    }
}
