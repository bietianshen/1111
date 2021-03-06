package com.hadoop.hdfs.mr.skew;


import com.hadoop.hdfs.mr.Util;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        Text outKey = new Text();
        IntWritable outValue =  new IntWritable();
        String[] args = value.toString().split(" ");
        for (String str : args) {
            outKey.set(str);
            outValue.set(1);
            context.write(outKey,outValue);
            context.getCounter("m",Util.getInfo(this,"map")).increment(1);
        }
    }
}
