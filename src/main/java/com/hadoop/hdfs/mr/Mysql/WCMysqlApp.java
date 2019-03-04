package com.hadoop.hdfs.mr.Mysql;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;


public class WCMysqlApp {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
      /*  conf.set("fs.defaultFS","file:///");*/
        Job job = Job.getInstance(conf);
        job.setJobName("WCMysqlApp");                        //工作名称
        job.setJarByClass(WCMysqlApp.class);
        String driverclass = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://192.168.1.241:3306/test" ;
        String username= "root" ;
        String password = "!@#QWEqwe" ;
        DBConfiguration.configureDB(job.getConfiguration(),driverclass,url,username,password);
        DBInputFormat.setInput(job,MyDBWritable.class,"select * from words ","select count(1) from words ");

        DBOutputFormat.setOutput(job,"stats","word","number");


       // FileOutputFormat.setOutputPath(job,new Path("C:\\Users\\liq\\Desktop\\MR\\sql"));
        job.setMapperClass(WCMysqlMapper.class);
        job.setReducerClass(WCMysqlReducer.class);

        job.setNumReduceTasks(3);                          //设置reduce个数
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(MyDBWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.waitForCompletion(true);
    }

}

