package com.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

public class HdfsDome {

    public static void main(String[] args) throws Exception {

    /*    temp("1.txt");
        temp("2.txt");
        temp("3.txt");
        temp("4.txt");
        temp("5.txt");*/
        seq("1.seq");
        seq("2.seq");
        seq("3.seq");
        seq("4.seq");
        seq("5.seq");
    }


    public static void temp(String name)throws Exception {
        int yead = 1970;
        Random rand = new Random();
        FileOutputStream out = new FileOutputStream("C:\\Users\\liq\\Desktop\\temp\\"+name);
        for(int i=0;i<100;i++){
            int num = rand.nextInt(100) + 1;
            String str = yead +" "+ num ;
            out.write(str.getBytes());
            out.write("\r\n".getBytes());
            yead++;
        }
        out.close();
    }


    public static void seq(String name) throws IOException {
        int yead = 1970;
        Random rand = new Random();
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","file:///");
        FileSystem fileSystem = FileSystem.get(conf);
        Path path = new Path("C:\\Users\\liq\\Desktop\\MR\\seq\\"+name);
        SequenceFile.Writer sequenceFile =  SequenceFile.createWriter( fileSystem,  conf, path, IntWritable.class, IntWritable.class);
        for (int i = 0; i < 1000; i++) {
            sequenceFile.append(new IntWritable(yead),new IntWritable(rand.nextInt(100) + 1));
            yead++;
        }
        sequenceFile.close();
    }


}
