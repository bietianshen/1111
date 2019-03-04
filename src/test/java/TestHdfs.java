import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;


public class TestHdfs {



    @Test
    public void testRead() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.94.200:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        Path path = new Path("/user/liqi/1.txt");
        FSDataInputStream  fsDataInputStream = fileSystem.open(path);
        ByteArrayOutputStream byteBufferOutputStream = new ByteArrayOutputStream();
        IOUtils.copyBytes(fsDataInputStream,byteBufferOutputStream,1024);
        System.out.println(new String(byteBufferOutputStream.toByteArray()));
    }


    @Test
    public void testAppend() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.94.200:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        Path path = new Path("/user/liqi/1.txt");
        FSDataInputStream fsDataInputStream = fileSystem.open(path);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        IOUtils.copyBytes(fsDataInputStream,byteArrayOutputStream,1024);

        FSDataOutputStream fsDataOutputStream = fileSystem.create(path);

        fsDataOutputStream.write(new String( byteArrayOutputStream.toString()+" hello world").getBytes());



    }

    @Test
    public void testWrite() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.94.200:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        Path path = new Path("/user/liqi/2.txt");
        FSDataOutputStream inputStream = fileSystem.create(path,true,1024,(short)4,1024);
        FileInputStream fileInputStream = new FileInputStream("C:\\Users\\liq\\Desktop\\新建文本文档 (2).txt");
         IOUtils.copyBytes(fileInputStream,inputStream,1024);

        inputStream.write("hello".getBytes());

    }


    @Test
    public void testAdd() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.94.200:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        Path path = new Path("/user/liqi/hello.txt");
        FSDataOutputStream inputStream = fileSystem.create(path);
        FileInputStream fileInputStream = new FileInputStream("C:\\Users\\liq\\Desktop\\新建文本文档 (2).txt");
        IOUtils.copyBytes(fileInputStream,inputStream,1024);
        fileInputStream.close();
        inputStream.close();
        fileSystem.close();
    }





    @Test
    public void testWrite2() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.94.200:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        Path path = new Path("/user/liqi/testHadoop/1.txt");
        FSDataOutputStream fsDataOutputStream = fileSystem.create(path);
        fsDataOutputStream.write(new String( " hello world").getBytes());
        fsDataOutputStream.flush();
    }



}
