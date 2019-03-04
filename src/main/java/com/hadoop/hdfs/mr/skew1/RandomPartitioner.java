package com.hadoop.hdfs.mr.skew1;


import org.apache.hadoop.mapreduce.Partitioner;

import java.util.Random;

public class RandomPartitioner extends Partitioner {

    public int getPartition(Object o, Object o2, int i) {
        return new Random().nextInt(i);//设置随机分区函数，将每个map随机的分配到不同的区中
    }
}
