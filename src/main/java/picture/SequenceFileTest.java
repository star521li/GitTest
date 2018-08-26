package picture;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.net.URI;

/**
 * @author star
 * describe: 使用Java将图片生成sequence file并保存到HBase
 * @date 2018/8/25
 */

public class SequenceFileTest {
    //HDFS路径
    static String inpath = "/picture/picHbase";
    static String outpath = "/picture/out";
    static SequenceFile.Writer writer = null;
    static HTable htable = null;

    public static void main(String[] args) throws Exception{
        System.setProperty("HADOOP_USER_NAME","centos");
        //inpath = args[0];

        //outpath = args[1];

        //String zklist = args[2];
        //HBase入库
        Configuration hbaseConf = HBaseConfiguration.create();
        Connection hconn = ConnectionFactory.createConnection(hbaseConf);
        //获得数据库表
      Table htable = hconn.getTable(TableName.valueOf("hpicture:picHbase"));
        //hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
        //hbaseConf.setStrings("hbase.zookeeper.quorum", "ip-192-163-116-101.ap-southeast-1.compute.internal");
        //指定表名
       // htable = new HTable(hbaseConf,"picHbase");


        /*//初始化hbase 的conf
        Configuration conf = HBaseConfiguration.create();
        //通过连接工厂创建连接
        Connection conn = ConnectionFactory.createConnection(conf);
        //得到相应数据库的表
        HTable table = (HTable) conn.getTable(TableName.valueOf("test:t1"));*/


        //设置读取本地磁盘文件
        Configuration conf = new Configuration();
        //conf.addResource(new Path("C:\\Users\\17534\\eclipse-workspace\\hbaseexmaple\\core-site.xml"));

        //conf.addResource(new Path("C:\\Users\\17534\\eclipse-workspace\\hbaseexmaple\\hdfs-site.xml"));
        URI uri = new URI(inpath);
        FileSystem fileSystem = FileSystem.get(uri, conf);

        //实例化writer对象
        writer = SequenceFile.createWriter(fileSystem, conf, new Path(outpath), Text.class, BytesWritable.class);
        //递归遍历文件夹，并将文件下的文件写入sequenceFile文件
        listFileAndWriteToSequenceFile(fileSystem,inpath);
        //关闭流
        org.apache.hadoop.io.IOUtils.closeStream(writer);
        //读取所有文件

        URI seqURI = new URI(outpath);

        FileSystem fileSystemSeq = FileSystem.get(seqURI, conf);

        SequenceFile.Reader reader = new SequenceFile.Reader(fileSystemSeq, new Path(outpath), conf);

        Text key = new Text();
        BytesWritable val = new BytesWritable();

//        key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), conf);

//        val = (BytesWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);

        int i = 0;
        while(reader.next(key, val)){

            String temp = key.toString();
            temp = temp.substring(temp.lastIndexOf("/") + 1);

//            temp = temp.substring(temp.indexOf("Image")+6, temp.indexOf("."));

//            String[] tmp = temp.split("/");

            //rowKey 设计

            String rowKey = temp;

//            String rowKey = Integer.valueOf(tmp[0])-1+"_"+Integer.valueOf(tmp[1])/2+"_"+Integer.valueOf(tmp[2])/2;

            System.out.println(rowKey);
            //指定ROWKEY的值
            Put put = new Put(Bytes.toBytes(rowKey));

            //指定列簇名称、列修饰符、列值 temp.getBytes()
            put.addColumn("picinfo".getBytes(), "content".getBytes() , val.getBytes());

            htable.put(put);
        }
        htable.close();
        org.apache.hadoop.io.IOUtils.closeStream(reader);
    }

    /****

     * 递归文件;并将文件写成SequenceFile文件

     * @param fileSystem

     * @param path

     * @throws Exception

     */

    public static void listFileAndWriteToSequenceFile(FileSystem fileSystem,String path) throws Exception{
        final FileStatus[] listStatuses = fileSystem.listStatus(new Path(path));

        for (FileStatus fileStatus : listStatuses) {
            if(fileStatus.isFile()){
                //图片路径名 Text-->string
                Text fileText = new Text(fileStatus.getPath().toString());

                System.out.println(fileText.toString());
                //返回一个SequenceFile.Writer实例 需要数据流和path对象 将数据写入了path对象
                FSDataInputStream in = fileSystem.open(new Path(fileText.toString()));
                byte[] buffer = IOUtils.toByteArray(in);
                in.read(buffer);
                BytesWritable value = new BytesWritable(buffer);
                //写成SequenceFile文件
                writer.append(fileText, value);
            }
            if(fileStatus.isDirectory()){
                listFileAndWriteToSequenceFile(fileSystem,fileStatus.getPath().toString());
            }
        }

    }

}
