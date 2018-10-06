package HBaseEtc;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import reader.HDFSClient;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Date;

public class T5 {


    public static void main(String[] args){
        try {
            Long timeStart = new Date().getTime();
            System.out.println(timeStart);
            HBase hBase = new HBase();
            HDFSClient hdfsClient = new HDFSClient();
            InputStream inputStream = hdfsClient.readAsInputStream("hdfs://10.141.209.224:9000/bodyDemo/it5.txt");
            Table tableID = hBase.getTable("ID-TimestampDemo");
//            Table tableTS = hBase.getTable("Timestamp-ID");
            Table assistTable = hBase.getTable("AssistTable");
            System.out.println(" link to table successfully");
            hBase.streamDataImport(tableID,inputStream,"T5", 55, "=",44, 1, assistTable,"hdfs://10.141.209.224:9000/testResult/it5.txt");
            Long timeEnd = new Date().getTime();
            System.out.println(timeEnd-timeStart);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
