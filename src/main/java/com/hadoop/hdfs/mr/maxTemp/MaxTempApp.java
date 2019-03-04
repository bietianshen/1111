package com.hadoop.hdfs.mr.maxTemp;

import com.hadoop.hdfs.mr.WCApp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MaxTempApp {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","file:///");
        Job job = Job.getInstance(conf);
        job.setJobName("MaxTempApp");                        //工作名称
        job.setJarByClass(WCApp.class);                 //搜索类
        job.setInputFormatClass(SequenceFileInputFormat.class); //设置格式化

        FileInputFormat.addInputPath(job,new Path("C:\\Users\\liq\\Desktop\\MR\\seq"));
        FileOutputFormat.setOutputPath(job,new Path("C:\\Users\\liq\\Desktop\\MR\\out"));
        job.setMapperClass(MaxTampMapper.class);
        job.setReducerClass(MaxTempReducer.class);

        //分区函数，在map之后进行一次相应的分区，在把分区结果发送给reduce
         job.setPartitionerClass(MyTemPPartitioner.class);

        job.setNumReduceTasks(3);                          //设置reduce个数
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);
    }

}

