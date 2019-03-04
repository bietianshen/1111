package com.hadoop.hdfs.mr.Mysql;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WCMysqlReducer extends Reducer<Text, IntWritable, MyDBWritable, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int num = 0;
        for(IntWritable iterable : values){
            num+=iterable.get();
        }
        MyDBWritable myDBWritable = new MyDBWritable();
        myDBWritable.setWord(key.toString());
        myDBWritable.setNumber(num);
        context.write(myDBWritable,  NullWritable.get());
    }
}
