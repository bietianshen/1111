package com.hadoop.hdfs.mr.maxTemp.allsort.secondarysort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MaxTempApp {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","file:///");
        Job job = Job.getInstance(conf);
        job.setJobName("MaxTempApp");                        //工作名称
        job.setJarByClass(MaxTempApp.class);                 //搜索类
        job.setInputFormatClass(TextInputFormat.class); //设置格式化

        FileInputFormat.addInputPath(job,new Path("C:\\Users\\liq\\Desktop\\temp\\data"));
        FileOutputFormat.setOutputPath(job,new Path("C:\\Users\\liq\\Desktop\\temp\\out"));
        job.setMapperClass(MaxTampMapper.class);
        job.setReducerClass(MaxTempReducer.class);

        //设置map输出参数
        job.setMapOutputKeyClass(ComboKey.class);
        job.setMapOutputValueClass(NullWritable.class);

        //设置reduce输出参数
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        //设置分区类
        //  job.setPartitionerClass(YearPartitioner.class);
        //设置分组对比器
        job.setGroupingComparatorClass(YearGroupComparator.class);
        //设置排序对比器
        job.setSortComparatorClass(ComboKeyComparator.class);

        job.setNumReduceTasks(3);                          //设置reduce个数




        /* 创建随机采样器
         * freq：每个key被选中的概率
         * numSamples：抽取样本的总数
         * maxSplitsSampled：最大采样切片数
         */
      /* InputSampler.Sampler sampler =
                new InputSampler.RandomSampler<IntWritable,IntWritable>
                        (1, 50000,3);

        //将samples数据写入分区文件，设置分区文件必须在写入分割文件之前，否自会去默认的lst文件
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(),new Path("C:\\Users\\liq\\Desktop\\MR\\par.lst"));

        InputSampler.writePartitionFile(job,sampler);

        //设置全排序类分区
        job.setPartitionerClass(TotalOrderPartitioner.class);*/

        job.waitForCompletion(true);
    }

}

