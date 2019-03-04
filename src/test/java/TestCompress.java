import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;

public class TestCompress {

    @Test
    public void deflateCompress() throws Exception {
        Class[] classes = {
                DeflateCodec.class,
                GzipCodec.class,
                BZip2Codec.class,
                Lz4Codec.class,
                SnappyCodec.class
        };
        for(Class clas : classes){
           unzip(clas);
        }
        System.out.println("===================");
        for(Class clas : classes){
            unzip(clas);
        }
    }

    public void zip( Class codecClass ) throws Exception {
        long start = System.currentTimeMillis();
        //实例化对象
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass,new Configuration());
        //创建文件输出流
        FileOutputStream  stream = new FileOutputStream("C:/Users/liq/Desktop/MR/1"+codec.getDefaultExtension());
        CompressionOutputStream zipOut = codec.createOutputStream(stream);
        IOUtils.copyBytes(new FileInputStream("C:/Users/liq/Desktop/tj.sql"),zipOut,1024);
        zipOut.close();
        System.out.println(codecClass.getSimpleName() + ":" + (System.currentTimeMillis()  -  start));
    }



    public void unzip( Class codecClass ) throws Exception {
        long start = System.currentTimeMillis();
        //实例化对象
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass,new Configuration());
        //创建文件输出流
        FileInputStream  stream = new FileInputStream("C:/Users/liq/Desktop/MR/1"+codec.getDefaultExtension());
        CompressionInputStream zipin = codec.createInputStream(stream);
        IOUtils.copyBytes(zipin,new FileOutputStream("C:/Users/liq/Desktop/MR/1"+codec.getDefaultExtension()+".txt"),1024);
        zipin.close();
        System.out.println(codecClass.getSimpleName() + ":" + (System.currentTimeMillis()  -  start));
    }


}
