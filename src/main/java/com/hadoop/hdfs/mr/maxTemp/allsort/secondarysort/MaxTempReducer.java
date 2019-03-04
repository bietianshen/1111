package com.hadoop.hdfs.mr.maxTemp.allsort.secondarysort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxTempReducer extends Reducer<ComboKey, NullWritable, IntWritable, IntWritable> {

    @Override
    protected void reduce(ComboKey key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        int year = key.getYear();
        int temp = key.getTemp();
        for(NullWritable iterable : values){
            System.out.println(year + ":" + temp);
        }
        context.write(new IntWritable(year), new IntWritable(temp));
    }
}
