package com.hadoop.hdfs.mr.multiinput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WCApp {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","file:///");
        Job job = Job.getInstance(conf);
        job.setJobName("MaxTempApp");                        //工作名称
        job.setJarByClass(WCApp.class);                 //搜索类


        //多个输入
        MultipleInputs.addInputPath(job, new Path("file:///C:\\Users\\liq\\Desktop\\MR\\seq"), SequenceFileInputFormat.class,WCSeqMapper.class);
        MultipleInputs.addInputPath(job, new Path("file:///C:\\Users\\liq\\Desktop\\MR\\text"), TextInputFormat.class,WCTestMapper.class);

        //设置输出目录
        FileOutputFormat.setOutputPath(job,new Path("C:/Users/liq/Desktop/MR/out"));

        job.setReducerClass(WCReducer.class);

        job.setNumReduceTasks(3);                          //设置reduce个数
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(false);
    }

}

