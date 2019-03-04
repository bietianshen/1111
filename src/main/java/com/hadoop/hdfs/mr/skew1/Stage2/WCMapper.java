package com.hadoop.hdfs.mr.skew1.Stage2;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] args = value.toString().split("\t");
        context.write(new Text(args[0]),new IntWritable(Integer.parseInt(args[1])));
    }
}
