package HBaseEtc;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import reader.HDFSClient;

import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;
import java.util.*;


///HBASE 2.0

public class HBase {
    private static Configuration conf;
    static Connection connection;
    private static Admin admin;
    private static FileSystem fileSystem;

    public HBase() throws Exception {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.141.209.224");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.master", "10.141.209.224:60000");
        conf.set("fs.defaultFS", "hdfs://10.141.209.224:9000");
        fileSystem = FileSystem.newInstance(conf);
        connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();
    }


    public static void main(String[] args) {
        try {
            HBase HBase = new HBase();
            HDFSClient hdfsClient = new HDFSClient();
            HBase.tableCreate("Demo", new String[]{"Info"});

//            byte[] txtArray = hdfsClient.readAsByteArray("hdfs://10.141.209.224:9000/bodyDemo/t6.txt");
//            HBaseEtc.HBase.importByteArrayData(TableName.valueOf("phoneEnrollInfo"), txtArray,"Info",new String[]{"name", "phoneNum", "phoneState", "ts1", "ts2", "ts3","ts4","ts5", "ts6"},"=");

//            InputStream inputStream = hdfsClient.readAsInputStream("hdfs://10.141.209.224:9000/bodyDemo/t6.txt");
//            HBaseEtc.HBase.importStreamData(TableName.valueOf("phoneEnrollInfo"), inputStream, "Info",new String[]{"name", "phoneNum", "phoneState", "ts1", "ts2", "ts3","ts4","ts5", "ts6"},"=");

//            HBaseEtc.HBase.tableDrop("phoneEnrollInfo");
//            HBaseEtc.HBase.rowDelete("dbtest",new String[]{"rk001"});
//            HBaseEtc.HBase.tableScan("phoneEnrollInfoDemo");
        } catch (IOException exception) {
            exception.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    Table getTable(String tableNameString) throws IOException {
        return connection.getTable(TableName.valueOf(tableNameString));
    }


    public void tableCreate(String tableNameString, String[] familyColumnNames) throws IOException {
//        Connection connection = ConnectionFactory.createConnection(conf);
//        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf(tableNameString);
        if (admin.tableExists(tableName)) {
            System.out.println(tableNameString + "has existed...");
        }
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
        Set<ColumnFamilyDescriptor> columnFamilyDescriptors = new HashSet<>();
        for (String familyName : familyColumnNames) {
            columnFamilyDescriptors.add(ColumnFamilyDescriptorBuilder.of(familyName));
        }
        tableDescriptorBuilder.setColumnFamilies(columnFamilyDescriptors);
        admin.createTable(tableDescriptorBuilder.build());
        System.out.println("Table " + tableNameString + " has been created successfully!");
    }

    // 小数据量时，导入数据
    public void importByteArrayData(String tableNameString, byte[] txtArray, String cf, String[] columns, String regex) throws IOException {
        Table table = this.getTable(tableNameString);

        String Str = new String(txtArray);
        String[] txtLine = Str.split("\n");
        for (String txt : txtLine) {
            String rowkey = txt.split(regex)[0];
            Put put = new Put(Bytes.toBytes(rowkey));
            int len = columns.length;
            for (int i = 1; i <= len; i++) {
                put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(columns[i - 1]), Bytes.toBytes(txt.split(regex)[i]));
                System.out.println(txt.split(regex)[i]);
            }
            table.put(put);
        }
        System.out.println("Import data successfully!");
    }


    //    数据量大时，导入数据用 数据流 的方式导入
    public void importStreamData(String tableNameString, InputStream dataStream, String cf, String[] columns, String regex) throws IOException {
        Table table = this.getTable(tableNameString);
        InputStreamReader inputStreamReader = new InputStreamReader(dataStream);
        BufferedReader reader = new BufferedReader(inputStreamReader);
        String line;
        Long lineCounter = 0l;
        while((line = reader.readLine()) != null){
            lineCounter++;
            String[] lineSplit = line.split(regex);
            String rowkey = lineSplit[0];
            Put put = new Put(Bytes.toBytes(rowkey));
            int len = columns.length;
            for (int i = 1; i <= len; i++) {
                put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(columns[i - 1]), Bytes.toBytes(line.split(regex)[i]));
            }
            table.put(put);
            if(lineCounter%100000==0){
                System.out.println(lineCounter);
                for (int i = 1; i <= len; i++) {
                    System.out.println(line.split(regex)[i]);
                }
                System.out.println();
            }
        }
        System.out.println("Import data successfully!");
    }


    public void tableDrop(String tableNameString) throws IOException {
        TableName tableName = TableName.valueOf(tableNameString);
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        System.out.println(tableName + " has been dropped!");
    }

    public void rowDelete(String tableNameString, String[] rowkeys) throws IOException {
        Table table = this.getTable(tableNameString);
        for (String rowkey : rowkeys) {
            Delete d1 = new Delete(rowkey.getBytes());
            table.delete(d1);
        }
        System.out.println("Some rows have been deleted!");
    }


    //  浏览表中数据
    void tableScan(String tableNameString) throws IOException {
        System.out.println("Scanning table " + tableNameString + "...:");
        Table table = this.getTable(tableNameString);
        Scan scan = new Scan();

        ResultScanner resultScanners = table.getScanner(scan);
        for (Result resultScanner : resultScanners) {
            System.out.println("RowKey:" + new String(resultScanner.getRow()));
            Map<String, Map<String, String>> resultMap = new HashMap<>();
            resultScanner.listCells().forEach(
                    cell -> {
                        int familyOffset = cell.getFamilyOffset();
                        int familyLength = cell.getFamilyLength();
                        byte[] fullFamily = cell.getFamilyArray();
                        String familyString = new String(ArrayUtils.subarray(fullFamily, familyOffset, familyOffset + familyLength));

                        int qualifierOffset = cell.getQualifierOffset();
                        int qualifierLength = cell.getQualifierLength();
                        byte[] fullQualifier = cell.getQualifierArray();
                        String qualifierString = new String(ArrayUtils.subarray(fullQualifier, qualifierOffset, qualifierOffset + qualifierLength));

                        int valueOffset = cell.getValueOffset();
                        int valueLength = cell.getValueLength();
                        byte[] fullValue = cell.getValueArray();
                        String valueString = new String(ArrayUtils.subarray(fullValue, valueOffset, valueOffset + valueLength));

                        resultMap.putIfAbsent(familyString, new HashMap<>());
                        resultMap.get(familyString).putIfAbsent(qualifierString, valueString);
                    }
            );
            resultMap.forEach((family, qualifierAndValueMap) -> {
                int familyLength = family.length();
                boolean outputFamily = true;
                Set<String> keySet = qualifierAndValueMap.keySet();
                List<String> keyList = new ArrayList<>(keySet);
                keyList.sort(Comparator.naturalOrder());
                for(String qualifier: keyList){
                    if (outputFamily){
                        System.out.print(family);
                        outputFamily = false;
                    }else {
                        for(int i = 0; i < familyLength; i++){
                            System.out.print(" ");
                        }
                    }
                    System.out.println(" -> " + qualifier + " -> " + qualifierAndValueMap.get(qualifier));
                }
                System.out.println();
            });
        }
    }



    public String[] QueryOneInStringTableName(String tableNameString, String rowKey){
        List<String> valuesString = new ArrayList<>();
        try {
            Table table = getTable(tableNameString);
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            valuesString = ValuesByteToString(result);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return  valuesString.toArray(new String[10]);
    }


    public String[] QueryOneInTable(Table table, String rowKey) throws IOException {
        List<String> valuesString = new ArrayList<>();
        try {
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            if(result.size() == 0){
                System.out.println("找不到结果");
                return null;
            }else {
                valuesString = ValuesByteToString(result);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return  valuesString.toArray(new String[10]);
    }


    public List<String> ValuesByteToString(Result result){
        List<String> resultList = new ArrayList<>();
        List<Cell> cellList = result.listCells();
        for (Cell cell:cellList){
            int valueOffset = cell.getValueOffset();
            int valueLength = cell.getValueLength();
            byte[] fullValue = cell.getValueArray();
            String valueString = new String(ArrayUtils.subarray(fullValue, valueOffset, valueOffset + valueLength));
            resultList.add(valueString);
        }
        return resultList;
    }

    public void streamDataImport(Table tableID, Table tableTS, InputStream inputStream, String columnFamily, Integer
            totalColumns, String regex, Integer orderOfPhoneNum, Integer orderOfTimestamp, Table assistTable, String
                                         filePath) throws Exception {
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        String line;
        Long lineCounter = 0L;
        Long nullPoint =0L;
        // to write
        Path path = new Path(filePath);
        FileSystem fs = path.getFileSystem(conf);
        FSDataOutputStream file = fs.create(path);

        while((line = bufferedReader.readLine()) != null) {
            String[] lineSplit = line.split(regex);
            lineCounter++;
            String phoneNum = lineSplit[orderOfPhoneNum-1];
            String timestamp = lineSplit[orderOfTimestamp-1];
            HBase hBase = new HBase();
            String[] queryResult = hBase.QueryOneInTable(assistTable, phoneNum);
            if (queryResult == null){
                System.out.println(lineCounter);
                System.out.println("nullPoint sum : " + (++nullPoint));
                // to write
                file.write(Bytes.toBytes(lineCounter+ " has no result"));
                file.flush();
            }else {
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

                if (lineCounter % 10000 == 0) {
                    System.out.println(lineCounter);
                }
            }
        }
        // to write
        file.write(Bytes.toBytes("There are "+ lineCounter + "lines ."));
        file.write(Bytes.toBytes("But there are "+ nullPoint + "lines have no results"));
        file.close();
        System.out.println("Import data successfully!");
    }




}
