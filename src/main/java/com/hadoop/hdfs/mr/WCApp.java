package com.hadoop.hdfs.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


public class WCApp {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // conf.set("fs.defaultFS","file:///");
        Job job = Job.getInstance(conf);
        job.setJobName("WCApp");                        //工作名称
        job.setJarByClass(WCApp.class);                 //搜索类
        job.setInputFormatClass(TextInputFormat.class); //设置格式化

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

/*
        FileInputFormat.setMaxInputSplitSize(job,10);
        FileInputFormat.setMinInputSplitSize(job,1);
*/

        // job.setPartitionerClass(MyPartitioner.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);

        job.setNumReduceTasks(3);                          //设置reduce个数
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);
    }

}

