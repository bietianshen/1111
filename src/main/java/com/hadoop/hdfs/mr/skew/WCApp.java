package com.hadoop.hdfs.mr.skew;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WCApp {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","file:///");
        Job job = Job.getInstance(conf);
        job.setJobName("WCApp");                        //工作名称
        job.setJarByClass(WCApp.class);                 //搜索类
        job.setInputFormatClass(TextInputFormat.class); //设置格式化

        FileInputFormat.addInputPath(job,new Path("C:\\Users\\liq\\Desktop\\MR\\skew"));
        FileOutputFormat.setOutputPath(job,new Path("C:\\Users\\liq\\Desktop\\MR\\skew\\out"));
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);

        job.setNumReduceTasks(4);                          //设置reduce个数
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);
    }

}

