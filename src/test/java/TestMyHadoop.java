
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

/**
 *测试hdfs
 */
public class TestMyHadoop {


    public static void main(String[] args) throws Exception {
        //注册url流处理工厂
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        URL url = new URL("hdfs://192.168.94.200:8020/user/liqi/1.txt");
        URLConnection connection = url.openConnection();
        InputStream iStream = connection.getInputStream();
        byte[] bs = new byte[iStream.available()];
        iStream.read(bs);
        iStream.close();
        String string = new String(bs);
        System.out.println(string);
    }

    @Test
    public void testRead() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("hdfs://192.168.94.200:8020/user/liqi/1.txt") ;
        FSDataInputStream fis = fs.open(path);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IOUtils.copyBytes(fis,baos,1024);
        fis.close();
        System.out.println(new String(baos.toByteArray()));

       // testWrite();
    }

    /**
     * 写入
     */
    @Test
    public void testWrite() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("hdfs://192.168.94.200:8020/user/liqi/hello.txt") ;
        FSDataOutputStream fout = fs.create(path);
        fout.write("how are you?".getBytes());

    }

    /**
     * 定制副本数和blocksize
     */
    @Test
    public void testWrite2() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream fout = fs.create(new Path("/user/centos/a.txt"),
                true, 1024, (short) 2,
                1024);

        FileInputStream fis = new FileInputStream("d:/a.txt");
        IOUtils.copyBytes(fis,fout,1024);
        fout.close();
        fis.close();
    }
}
