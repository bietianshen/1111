package com.hadoop.hdfs.mr.maxTemp.allsort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxTempReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int max = 0;
        for(IntWritable iterable : values){
            max =  max > iterable.get() ?max:iterable.get();
        }
        context.write(key, new IntWritable(max));
    }
}
