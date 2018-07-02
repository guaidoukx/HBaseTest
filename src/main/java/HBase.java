import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HBaseFsck;
import reader.HDFSClient;
import org.apache.commons.codec.binary.Hex.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;
import java.util.stream.Collectors;

import static com.sun.org.apache.xalan.internal.xsltc.compiler.util.Type.Int;

public class HBase {
    static Configuration conf;

    public HBase() throws Exception {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.141.209.224");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.master", "10.141.209.224:60000");
        conf.set("fs.defaultFS", "hdfs://10.141.209.224:9000");
//        HBaseAdmin admin = new HBaseAdmin(conf);
    }

    public static void main(String[] args) {
        try {
            HBase HBase = new HBase();
            HDFSClient hdfsClient = new HDFSClient("hdfs://10.141.209.224:9000");
//            HBase.tableCreate("phoneEnrollInfo", new String[]{"Info"});
//            byte[] txtArray = hdfsClient.readAsByteArray("hdfs://10.141.209.224:9000/bodyDemo/t6.txt");
//            HBase.dataImport(TableName.valueOf("phoneEnrollInfo"), txtArray,"Info",new String[]{"name", "phoneNum", "phoneState", "ts1", "ts2", "ts3","ts4","ts5", "ts6"},"=");
//            HBase.tableTrop("test");
//            HBase.rowDelete("dbtest",new String[]{"rk001"});
            HBase.tableScan("phoneEnrollInfoDemo");
        } catch (IOException exception) {
            exception.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void tableCreate(String tableNameString, String[] familyColumnNames) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
//        HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
        TableName tableName = TableName.valueOf(tableNameString);
        if (admin.tableExists(tableName)) {
            System.out.println(tableNameString + "has existed...");
        }
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
//        ColumnFamilyDescriptorBuilder familyDescriptorBuilder = ColumnFamilyDescriptorBuilder;
        Set<ColumnFamilyDescriptor> columnFamilyDescriptors = new HashSet<>();
        for (String familyName : familyColumnNames) {
            columnFamilyDescriptors.add(ColumnFamilyDescriptorBuilder.of(familyName));
        }
        tableDescriptorBuilder.setColumnFamilies(columnFamilyDescriptors);
        admin.createTable(tableDescriptorBuilder.build());
        System.out.println("Table " + tableNameString + " has been created successfully!");
    }


    public void dataImport(TableName tableName, byte[] txtArray, String cf, String[] columns, String regex) throws IOException {
//        HTable table = new HTable(conf, tableName);
//        table.setAutoFlushTo(true);
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        Table table = connection.getTable(tableName);

        String Str = new String(txtArray);
        String[] txtLine = Str.split("\n");
        List<Put> lists = new ArrayList<Put>();
        for (String txt : txtLine) {
            String rowkey = txt.split(regex)[0];
            Put put = new Put(Bytes.toBytes(rowkey));
            int len = columns.length;
            for (int i = 1; i <= len; i++) {
                put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(columns[i - 1]), Bytes.toBytes(txt.split(regex)[i]));
//                put.addColumn(cf.getBytes(), columns[i-1].getBytes(), txt.split(regex)[i].getBytes());
                System.out.println(txt.split(regex)[i]);
            }
            table.put(put);
        }
        System.out.println("Import data successfully!");
    }


    public void tableTrop(String tableNameString) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf(tableNameString);
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        System.out.println(tableName + " has been dropped!");
    }

    public void rowDelete(String tableNameString, String[] rowkeys) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        Table table = connection.getTable(TableName.valueOf(tableNameString));
//        HTable table = new HTable(conf, tableName);
//        List list = new ArrayList();
        for (String rowkey : rowkeys) {
            Delete d1 = new Delete(rowkey.getBytes());
            table.delete(d1);
        }
//        list.add(d1);
        System.out.println("Some rows have been deleted!");
    }


    //    to scan a table
    public void tableScan(String tableNameString) throws IOException {
        System.out.println("Scanning table " + tableNameString + "...:");
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        Table table = connection.getTable(TableName.valueOf(tableNameString));
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

//        Scan.createScanFromCursor();
    }

//    public static void QueryAll(String tableName) {
//        HTablePool pool = new HTablePool(conf, 1000);
//        HTable table = (HTable) pool.getTable(tableName);
//        try {
//            ResultScanner rs = table.getScanner(new Scan());
//            for (Result r : rs) {
//                System.out.println("???rowkey:" + new String(r.getRow()));
//                for (KeyValue keyValue : r.raw()) {
//                    System.out.println("??" + new String(keyValue.getFamily())
//                            + "====?:" + new String(keyValue.getValue()));
//                }
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }


}
