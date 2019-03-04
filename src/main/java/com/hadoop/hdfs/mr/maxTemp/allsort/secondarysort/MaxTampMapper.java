package com.hadoop.hdfs.mr.maxTemp.allsort.secondarysort;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxTampMapper extends Mapper<LongWritable, Text, ComboKey, NullWritable> {
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] args = value.toString().split(" ");
        System.out.println(value);
        Integer year =Integer.valueOf(args[0]);
        Integer temp = Integer.valueOf(args[1]);

        if( year == 2068 && temp == 96){
            System.out.println(123);
        }
        ComboKey comboKey = new ComboKey();
        comboKey.setYear(year);
        comboKey.setTemp(temp);
        context.write(comboKey,NullWritable.get());
    }
}
