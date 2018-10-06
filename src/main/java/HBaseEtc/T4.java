package HBaseEtc;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import reader.HDFSClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Date;

public class T4 {

    public void streamDataImport(HBase hBase,Table tableID, Table tableTS, InputStream inputStream, String columnFamily, Integer
            totalColumns, String regex, Integer orderOfPhoneNum, Integer orderOfTimestamp, Table assistTable) throws Exception {
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        String line;
        Long lineCounter = 0L;
        Long nullPoint =0L;
        while((line = bufferedReader.readLine()) != null) {
            lineCounter++;
            String[] lineSplit = line.split(regex);
            if (lineSplit.length == totalColumns) {
                String phoneNum = lineSplit[orderOfPhoneNum - 1];
                String timestamp = lineSplit[orderOfTimestamp - 1];
                String[] queryResult = hBase.QueryOneInTable(assistTable, phoneNum);
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
            } else {
                System.out.println("Wrong format sum : " + (++nullPoint) + " IndexOutOfBounds. ");
            }

            if (lineCounter == 1){
                System.out.println("--==Input " + lineCounter + " lines. ==--");
            }if (lineCounter % 100000 == 0) {
                System.out.println("--==Input " + lineCounter + " lines. ==--");
                System.out.println(new Date().getTime());
            }if (lineCounter >= 100000000)
                break;
        }
        System.out.println("Import data successfully!");
    }



    private void deleteT10inTimestamp_ID(HBase hBase, Table tableTS, InputStream inputStream, Integer totalColumns,
                                         Integer orderOfPhoneNum, Table assistTable) throws IOException {
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        String line;
        Long lineCounter = 0L;
        while ( (line = bufferedReader.readLine()) != null){
            lineCounter++;
            String[] lineSplit = line.split("=");
            if (lineSplit.length == totalColumns) {
                String phoneNum = lineSplit[orderOfPhoneNum - 1];
                String[] queryResult = hBase.QueryOneInTable(assistTable, phoneNum);
                String id = queryResult[0];
                Delete delete = new Delete(Bytes.toBytes(id));
                tableTS.delete(delete);
            }
            if (lineCounter == 1){
                System.out.println("--==Delete " + lineCounter + " lines. ==--");
            }if (lineCounter % 100000 == 0) {
                System.out.println("--==Delete " + lineCounter + " lines. ==--");
                System.out.println(new Date().getTime());
            }if (lineCounter >= 100000000)
                break;
        }
        System.out.println("Delete successfully");
    }

    public static void main(String[] args){
        try {
            System.out.println("----------This is T10.----------");
            HBase hBase = new HBase();
            T4 t4 = new T4();
            HDFSClient hdfsClient = new HDFSClient();
            InputStream inputStream = hdfsClient.readAsInputStream("hdfs://10.141.209.224:9000/bodyDemo/t10.txt");
//            Table tableID = hBase.getTable("ID-Timestamp");
            Table tableTS = hBase.getTable("Timestamp-ID");
            Table assistTable = hBase.getTable("AssistTable");
            System.out.println(" link to table successfully");
            Long timeStart = new Date().getTime();
            System.out.println(timeStart);
            t4.deleteT10inTimestamp_ID(hBase, tableTS, inputStream, 10, 5, assistTable);
            Long timeEnd = new Date().getTime();
            System.out.println(timeEnd-timeStart);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
