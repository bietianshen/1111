package com.hadoop.hdfs.mr.skew1.Stage3.Stage2;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WCMapper extends Mapper<Text, Text, Text, IntWritable> {
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        context.write(key,new IntWritable(Integer.valueOf(value.toString())));
    }
}
