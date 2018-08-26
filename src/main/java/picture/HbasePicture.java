package picture;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

/**
 * @author star
 * @date 2018/8/25
 */
public class HbasePicture {
    static SequenceFile.Writer  writer = null;
    //****///
    public static void main(String[] args) throws Exception {
        //输入文件夹路径  输出文件夹路径
        System.setProperty("HADOOP_USER_NAME","centos");
         Path inpath = new Path("/picture/picHbase");
         Path outpath = new Path("/picture/out");
        //配置文件
        Configuration conf = new Configuration();
        //获得文件系统
        FileSystem fileSystem = FileSystem.get(conf);
        //SequenceFile序列化文件 写入到目标文件
//////////////---------------------------
        //初始化hbase 的conf
        Configuration hconf = HBaseConfiguration.create();
        //通过连接工厂创建连接
        Connection hconn = ConnectionFactory.createConnection(hconf);
        //获得数据库表
        Table table = hconn.getTable(TableName.valueOf("hpicture:picHbase"));
/////////------------------------------------
        SequenceFile.Writer  writer = SequenceFile.createWriter(fileSystem, conf, outpath, IntWritable.class, Text.class, SequenceFile.CompressionType.BLOCK);
        //调取sequencefile生成函数
        writeSequenfile(fileSystem,inpath);
        //反序列化 读sequencefile文件
        SequenceFile.Reader reader = new SequenceFile.Reader(fileSystem, outpath, conf);
       //初始化两个writable对象
        Text key = new Text();
        BytesWritable value = new BytesWritable();
        //循环遍历读取sequencefile文件 取出 k  v
        while(reader.next(key,value)){
            //切出文件路径名最后那个图片名为rowkey
            String newkey = key.toString();
            String rowkey =   newkey.substring(newkey.lastIndexOf("/")+1);
            //把rowkey value 放hbase中
            //Bytes.toBytes(）可以将任意类型转换成字节数组
            System.out.println(rowkey);
            Put put = new Put(Bytes.toBytes(rowkey));   //row-key
            //向当前row-key的列族中添加数据
            put.addColumn(Bytes.toBytes("picinfo"), Bytes.toBytes("content"), value.getBytes());
            //
            table.put(put);

        }
        table.close();
        hconn.close();
    }
    //把hdfs目录文件循环遍历生成SequenceFile文件
    public static void writeSequenfile(FileSystem fileSystem, Path inpath) throws Exception {

        //遍历文件夹 把文件转换成sequencefile文件
        final FileStatus[] listStatuses = fileSystem.listStatus(inpath);
        for (FileStatus fileStatus : listStatuses) {
            //如果时文件 写进sequencefile
            if(fileStatus.isFile()){
                //图片路径名 Text-->string
                Text fileText = new Text(fileStatus.getPath().toString());
                //图片路径名打印出来
                System.out.println(fileText.toString());
                //返回一个SequenceFile.Writer实例 需要数据流和path对象 将数据写入了path对象
                //数据输入流
                FSDataInputStream in = fileSystem.open(new Path(fileText.toString()));
                byte[] buffer = IOUtils.toByteArray(in);//把图片转换成字节数组
                in.read(buffer);//读字节数组
                BytesWritable value = new BytesWritable(buffer);
                //写成SequenceFile文件 key为图片路径  value为图片字节数组
                try {
                    writer.append(fileText, value);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
            if(fileStatus.isDirectory()){
                writeSequenfile(fileSystem,new Path(fileStatus.getPath().toString()));
            }
        }


    }





}
