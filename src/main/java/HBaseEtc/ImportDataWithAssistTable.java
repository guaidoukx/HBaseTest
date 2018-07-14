package HBaseEtc;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import reader.HDFSClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;


public class ImportDataWithAssistTable {
    public HBase hBase;

    public ImportDataWithAssistTable() throws Exception {
        hBase = new HBase();
    }


    public static void main(String[] args){
        try {
            HBase hBase = new HBase();
            String fileSystemPath = "hdfs://10.141.209.224:9000";
            String filePath = "hdfs://10.141.209.224:9000/bodyDemo/t3.txt";
            HDFSClient hdfsClient = new HDFSClient(fileSystemPath);
            ImportDataWithAssistTable importDataWithAssistTable = new ImportDataWithAssistTable();

//            hBase.tableDrop("ID-TimestampDemo");
//            hBase.tableDrop("ID-Timestamp");
//            hBase.tableCreate("ID-TimestampDemo", new String[]{"T2"});
//            hBase.tableCreate("Timestamp-IDDemo", new String[]{"T2", "T3"});

            InputStream inputStream = hdfsClient.readAsInputStream(filePath);
            Table assistTable = hBase.connection.getTable(TableName.valueOf("AssistTable"));
            Table tableID = hBase.connection.getTable(TableName.valueOf("ID-TimestampDemo"));
//            Table tableTS = hBase.connection.getTable(TableName.valueOf("Timestamp-IDDemo"));
            importDataWithAssistTable.streamDataImport(tableID, inputStream, "T2", 45, "=", 24, 1, assistTable);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void queryAll() throws Exception {
//        System.out.println(hBase.QueryOneinTable(assistTable, "14550247097")[0]);


    }

    public void streamDataImport(Table tableID, InputStream inputStream, String columnFamily, Integer totalColumns, String regex, Integer orderOfPhoneNum, Integer orderOfTimestamp, Table assistTable) throws Exception {
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        String line;
        Long lineCounter = 0l;
        while((line = bufferedReader.readLine()) != null) {
            String[] lineSplit = line.split(regex);
            lineCounter++;
            String phoneNum = lineSplit[orderOfPhoneNum-1];
            String timestamp = lineSplit[orderOfTimestamp-1];
            HBase hBase = new HBase();
            String id = hBase.QueryOneinTable(assistTable, phoneNum)[0];
            Put putID = new Put(Bytes.toBytes(id + "-" + timestamp));
            Put putTS = new Put(Bytes.toBytes(timestamp + "-" + id));
            for (Integer i = 1; i <= totalColumns; i++) {
                if(!lineSplit[i - 1].equals(" ")){
                    if (i<10){
                        putID.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes('f' + "0" + i.toString()), Bytes.toBytes(lineSplit[i - 1]));
                        putTS.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes('f' + "0" + i.toString()), Bytes.toBytes(lineSplit[i - 1]));
                    }
                    else {
                        putID.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes('f' + i.toString()), Bytes.toBytes(lineSplit[i - 1]));
                        putTS.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes('f' + i.toString()), Bytes.toBytes(lineSplit[i - 1]));
                    }
                }
            }
            tableID.put(putID);
//            tableTS.put(putTS);

            if (lineCounter % 10000 == 0) {
                System.out.println(lineCounter);
//                for (int i = 1; i <= totalColumns; i++) {
//                    System.out.println(line.split(regex)[i]);
//                }
//                System.out.println();
            }
        }
        System.out.println("Import data successfully!");
    }



}
