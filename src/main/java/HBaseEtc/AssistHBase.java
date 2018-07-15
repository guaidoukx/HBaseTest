package HBaseEtc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import reader.HDFSClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;


///HBASE 2.0

public class AssistHBase {
    static Configuration conf;

    public AssistHBase() throws Exception {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.141.209.224");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.master", "10.141.209.224:60000");
        conf.set("fs.defaultFS", "hdfs://10.141.209.224:9000");
//        HBaseAdmin admin = new HBaseAdmin(conf);
    }


    public static void main(String[] args){
        try {
            HBase hBase = new HBase();
            AssistHBase assistHBase = new AssistHBase();
            HDFSClient hdfsClient = new HDFSClient();
            InputStream inputStream= hdfsClient.readAsInputStream("hdfs://10.141.209.224:9000/bodyDemo/t6.txt");
            hBase.tableCreate("AssistTable", new String[]{"ID"});
            assistHBase.importStreamData(TableName.valueOf("AssistTable"), inputStream, "ID", 2, new String[]{"id"}, new Integer[]{0}, "=");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void importStreamData(TableName tableName, InputStream dataStream, String cf, Integer rowkeyNum, String[] columns, Integer[] columnsNum , String regex) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        Table table = connection.getTable(tableName);
        InputStreamReader inputStreamReader = new InputStreamReader(dataStream);
        BufferedReader reader = new BufferedReader(inputStreamReader);
        String txt;
        Long lineCounter = 0l;

       while ((txt = reader.readLine()) != null){
           lineCounter++;
            String rowkey = txt.split(regex)[rowkeyNum];
            Put put = new Put(Bytes.toBytes(rowkey));
            int len = columns.length;
            for (int i = 0; i < len; i++) {
                put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(columns[i]), Bytes.toBytes(txt.split(regex)[columnsNum[i]]));
//                System.out.println(txt.split(regex)[i]);
            }
            table.put(put);
            if (lineCounter% 100000 ==0 ){
                System.out.println(lineCounter);
                System.out.println(txt.split(regex)[rowkeyNum]);
                System.out.println( txt.split(regex)[columnsNum[0]]);
            }
        }
        System.out.println("Import data successfully!");
    }


}
