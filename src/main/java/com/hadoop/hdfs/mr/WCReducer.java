package com.hadoop.hdfs.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int num = 0;
        for(IntWritable iterable : values){
            num+=iterable.get();
        }
        context.getCounter("r",Util.getInfo(this,"reduce")).increment(1);
        context.write(key, new IntWritable(num));
    }
}
