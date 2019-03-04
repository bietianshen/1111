package com.hadoop.hdfs.mr.multiinput;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WCSeqMapper extends Mapper<IntWritable, Text, Text, IntWritable> {
    protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {

        Text outKey = new Text();
        IntWritable outValue =  new IntWritable();
        String[] args = value.toString().split(" ");
        for (String str : args) {
            outKey.set(str);
            outValue.set(1);
            context.write(outKey,outValue);
        }
    }
}
