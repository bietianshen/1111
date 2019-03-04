package com.hadoop.hdfs.mr.Mysql;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WCMysqlMapper extends Mapper<LongWritable, MyDBWritable, Text, IntWritable> {
    protected void map(LongWritable key, MyDBWritable value, Context context) throws IOException, InterruptedException {
        Integer id =  value.getId();
        String name = value.getName();
        String text = value.getText();
        String [] args = text.split(" ");
        for(String str : args){
            System.out.println("id = "+id+ " ,name = " + name + " ,value = " + str);
            context.write(new Text(str) , new IntWritable(1));
        }
    }
}
