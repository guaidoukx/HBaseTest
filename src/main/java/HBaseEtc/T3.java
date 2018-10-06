package HBaseEtc;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import reader.HDFSClient;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;


public class T3 {

    public void streamDataImport(HBase hBase, Table tableID, Table tableTS, InputStream inputStream, String columnFamily, Integer
            totalColumns, String regex, Integer orderOfPhoneNum, Integer orderOfTimestamp, Table assistTable, String
                                         filePath) throws Exception {
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        String line;
        Long lineCounter = 0L;
//        Long nullPoint =0L;
        // to write
//        Path path = new Path(filePath);
//        FileSystem fs = path.getFileSystem(conf);
//        FSDataOutputStream file = fs.create(path);
        String[] lineSplit;
        String[] queryResult;
        while((line = bufferedReader.readLine()) != null) {
            lineCounter++;
            if (lineCounter>460000) {
                lineSplit = line.split(regex);//            if (lineSplit.length == totalColumns){
                String phoneNum = lineSplit[orderOfPhoneNum - 1];
                String timestamp = lineSplit[orderOfTimestamp - 1];
                queryResult = hBase.QueryOneInTable(assistTable, phoneNum);
//                if (queryResult == null) {
//                    System.out.println(lineCounter);
//                    System.out.println("Wrong format sum : " + (++nullPoint) + " nullPoint. ");
//                    // to write
//                    file.write(Bytes.toBytes(lineCounter + " has no result\n"));
//                    file.flush();
//                } else {
                String id = queryResult[0];
                Put putID = new Put(Bytes.toBytes(id + "-" + timestamp));
                Put putTS = new Put(Bytes.toBytes(timestamp + "-" + id));
                for (Integer i = 1; i <= totalColumns; i++) {
                    if (!lineSplit[i - 1].equals(" ")) {
                        if (i < 10) {
                            putID.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes('f' + "0" + i.toString()), Bytes.toBytes(lineSplit[i - 1]));
                            putTS.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes('f' + "0" + i.toString()), Bytes.toBytes(lineSplit[i - 1]));
                        } else {
                            putID.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes('f' + i.toString()), Bytes.toBytes(lineSplit[i - 1]));
                            putTS.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes('f' + i.toString()), Bytes.toBytes(lineSplit[i - 1]));
                        }
                    }
                }
                tableID.put(putID);
                tableTS.put(putTS);
//                }
//            }else {
//                System.out.println(lineCounter);
//                System.out.println("Wrong format sum : " + (++nullPoint) + " IndexOutOfBounds. ");
//                // to write
//                file.write(Bytes.toBytes(lineCounter + "array index out of bounds\n"));
//                file.flush();
//            }
                if (lineCounter % 100000 == 0) {
                    System.out.println("-------==Input " + lineCounter + " lines. ==--------");
                    System.out.println(new Date().getTime());
                }
                if (lineCounter >= 100000000)
                    break;
            }
        }
        // to write
//        file.write(Bytes.toBytes("There are "+ lineCounter + " lines in total.\n"));
//        file.write(Bytes.toBytes("But there are "+ nullPoint + " lines have format mistake. \n"));
//        file.close();
        System.out.println("Import data successfully!");
    }


    public static void main(String[] args){
        try {
            System.out.println("-------This is T3.---------");
            HBase hBase = new HBase();
            T3 t3 =new T3();
            HDFSClient hdfsClient = new HDFSClient();
            InputStream inputStream = hdfsClient.readAsInputStream("hdfs://10.141.209.224:9000/bodyDemo/t3.txt");
            //  测试 创建表
//            System.out.println(hBase.QueryOneInStringTableName("phoneEnrollInfoDemo", "520327198403131799")[0]);
            System.out.println(" link to table");
            Table tableID = hBase.getTable("ID-Timestamp");
            Table tableTS = hBase.getTable("Timestamp-ID");
            Table assistTable = hBase.getTable("AssistTable");
            System.out.println(" link to table successfully");
            Long timeStart = new Date().getTime();
            System.out.println(timeStart);
            t3.streamDataImport(hBase, tableID, tableTS, inputStream,"T3", 97, "=",26, 1, assistTable, "hdfs://10.141.209.224:9000/testResult/it3.txt");
            Long timeEnd = new Date().getTime();
            System.out.println(timeEnd-timeStart);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    // have been imported 9700w items.



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
