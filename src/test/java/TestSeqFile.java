import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 * 序列文件
 */
public class TestSeqFile {
    /**
     * 写操作
     */
    @Test
    public void save() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","file:///");
        FileSystem fileSystem = FileSystem.get(conf);
        Path path = new Path("C:\\Users\\liq\\Desktop\\MR\\seq\\1.seq");
        SequenceFile.Writer sequenceFile =  SequenceFile.createWriter( fileSystem,  conf, path, IntWritable.class, Text.class);
        for (int i = 0; i < 1000; i++) {
            sequenceFile.append(new IntWritable(i),new Text("liqi"+i));
            sequenceFile.sync();
        }
        for (int i = 0; i < 10; i++) {
            sequenceFile.append(new IntWritable(i),new Text("liqi"+i));
            if(i % 2 == 0){
                sequenceFile.sync();
            }
        }
        sequenceFile.close();
    }

    @Test
    public void read1() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","file:///");
        FileSystem fileSystem = FileSystem.get(conf);
        Path path = new Path("C:\\Users\\liq\\Desktop\\MR\\seq\\1.seq");

        SequenceFile.Reader reader =  new SequenceFile.Reader( fileSystem, path,  conf);
        IntWritable key = new IntWritable();
        Text value = new Text();
        while( reader.next(key,value)){
           System.out.println(key + " : " + value);
        }
        reader.close();
    }


    @Test
    public void read2() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","file:///");
        FileSystem fileSystem = FileSystem.get(conf);
        Path path = new Path("C:/Users/iq/Desktop/MR/seq/1.seq");

        SequenceFile.Reader reader =  new SequenceFile.Reader( fileSystem, path,  conf);
        IntWritable key = new IntWritable();
        Text value = new Text();
        while( reader.next(key)){
            reader.getCurrentValue(value);
            System.out.println( value.toString());
        }
        reader.close();
    }




    @Test
    public void read3() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","file:///");
        FileSystem fileSystem = FileSystem.get(conf);
        Path path = new Path("C:/Users/iq/Desktop/MR/seq/1.seq");
        SequenceFile.Reader reader =  new SequenceFile.Reader( fileSystem, path,  conf);
        //读取文件指针
        long pos = reader.getPosition();
        System.out.println(pos);

        IntWritable key = new IntWritable();
        Text value = new Text();
        while( reader.next(key,value)){
            System.out.println( reader.getPosition() + " ---- " + key + ":" + value);
        }
        reader.close();
    }


    /**
     *
     * @throws Exception
     */
    @Test
    public void read4() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","file:///");
        FileSystem fileSystem = FileSystem.get(conf);
        Path path = new Path("C:/Users/iq/Desktop/MR/seq/1.seq");
        SequenceFile.Reader reader =  new SequenceFile.Reader( fileSystem, path,  conf);

        IntWritable key = new IntWritable();
        Text value = new Text();

        reader.sync(660);//sync:找到下一个同步点，存入的时候设置了sync
        while(reader.next(key,value)){
            System.out.println( reader.getPosition() + " ---- " + key + ":" + value);
        }
        reader.close();
    }
}
