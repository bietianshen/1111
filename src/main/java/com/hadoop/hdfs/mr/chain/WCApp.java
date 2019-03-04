package com.hadoop.hdfs.mr.chain;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class WCApp {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
         conf.set("fs.defaultFS","file:///");
        Job job = Job.getInstance(conf);
        job.setJobName("WCApp");                        //工作名称
        job.setJarByClass(WCApp.class);                 //搜索类
        job.setInputFormatClass(TextInputFormat.class); //设置格式化

        job.setOutputFormatClass(TextOutputFormat.class);


        FileInputFormat.addInputPath(job,new Path("C:\\Users\\liq\\Desktop\\MR\\skew"));
        FileOutputFormat.setOutputPath(job,new Path("C:\\Users\\liq\\Desktop\\MR\\skew\\out"));

        ChainMapper.addMapper(job,WCMapper1.class, LongWritable.class, Text.class,Text.class, IntWritable.class, conf);
        ChainMapper.addMapper(job,WCMapper2.class, Text.class, IntWritable.class,Text.class, IntWritable.class, conf);

        ChainReducer.setReducer(job, WCReducer.class, Text.class, IntWritable.class,Text.class, IntWritable.class, conf);
        ChainReducer.addMapper(job, WCReducerMapper1.class, Text.class, IntWritable.class,Text.class, IntWritable.class, conf);

        job.setNumReduceTasks(3);                          //设置reduce个数

        job.waitForCompletion(true);
    }

}

