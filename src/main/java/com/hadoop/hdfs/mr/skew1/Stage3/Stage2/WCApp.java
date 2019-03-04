package com.hadoop.hdfs.mr.skew1.Stage3.Stage2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WCApp {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","file:///");
        Job job = Job.getInstance(conf);
        job.setJobName("WCApp");                        //工作名称
        job.setJarByClass(WCApp.class);                 //搜索类
        job.setInputFormatClass(KeyValueTextInputFormat.class); //设置格式化

        FileInputFormat.addInputPath(job,new Path("C:\\Users\\liq\\Desktop\\MR\\skew\\out\\part-r-00000"));
        FileInputFormat.addInputPath(job,new Path("C:\\Users\\liq\\Desktop\\MR\\skew\\out\\part-r-00001"));
        FileInputFormat.addInputPath(job,new Path("C:\\Users\\liq\\Desktop\\MR\\skew\\out\\part-r-00002"));
        FileInputFormat.addInputPath(job,new Path("C:\\Users\\liq\\Desktop\\MR\\skew\\out\\part-r-00003"));
        FileOutputFormat.setOutputPath(job,new Path("C:\\Users\\liq\\Desktop\\MR\\skew\\out2"));
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

