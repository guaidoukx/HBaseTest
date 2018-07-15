package HBaseEtc;

import org.apache.hadoop.hbase.client.Table;
import reader.HDFSClient;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;


public class ImportDataWithAssistTable {

    public static void main(String[] args){
        try {
            HBase hBase = new HBase();
            HDFSClient hdfsClient = new HDFSClient();
            InputStream inputStream = hdfsClient.readAsInputStream("hdfs://10.141.209.224:9000/bodyDemo/it3.txt");
            //  测试 创建表
//            System.out.println(hBase.QueryOneInStringTableName("phoneEnrollInfoDemo", "520327198403131799")[0]);
            Table tableID = hBase.getTable("ID-TimestampDemo");
            Table tableTS = hBase.getTable("Timestamp-IDDemo");
            Table assistTable = hBase.getTable("AssistTable");


            hBase.streamDataImport(tableID,tableTS,inputStream,"T3", 97, "=",26, 1, assistTable,
                    "hdfs://10.141.209.224:9000/testResult/it3.txt");
//

        } catch (Exception e) {
            e.printStackTrace();
        }
    }




//    public void streamDataImport(Table tableID, InputStream inputStream, String columnFamily, Integer totalColumns, String regex, Integer orderOfPhoneNum, Integer orderOfTimestamp, Table assistTable) throws Exception {
//        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
//        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
//        String line;
//        Long lineCounter = 0l;
//        while((line = bufferedReader.readLine()) != null) {
//            String[] lineSplit = line.split(regex);
//            lineCounter++;
//            String phoneNum = lineSplit[orderOfPhoneNum-1];
//            String timestamp = lineSplit[orderOfTimestamp-1];
//            HBase hBase = new HBase();
//            String id = hBase.QueryOneInTable(assistTable, phoneNum)[0];
//            Put putID = new Put(Bytes.toBytes(id + "-" + timestamp));
//            Put putTS = new Put(Bytes.toBytes(timestamp + "-" + id));
//            for (Integer i = 1; i <= totalColumns; i++) {
//                if(!lineSplit[i - 1].equals(" ")){
//                    if (i<10){
//                        putID.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes('f' + "0" + i.toString()), Bytes.toBytes(lineSplit[i - 1]));
//                        putTS.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes('f' + "0" + i.toString()), Bytes.toBytes(lineSplit[i - 1]));
//                    }
//                    else {
//                        putID.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes('f' + i.toString()), Bytes.toBytes(lineSplit[i - 1]));
//                        putTS.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes('f' + i.toString()), Bytes.toBytes(lineSplit[i - 1]));
//                    }
//                }
//            }
//            tableID.put(putID);
////            tableTS.put(putTS);
//
//            if (lineCounter % 10000 == 0) {
//                System.out.println(lineCounter);
////                for (int i = 1; i <= totalColumns; i++) {
////                    System.out.println(line.split(regex)[i]);
////                }
////                System.out.println();
//            }
//        }
//        System.out.println("Import data successfully!");
//    }
//


}
