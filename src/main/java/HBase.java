import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.HBaseFsck;
import reader.HDFSClient;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;


import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;

public class HBase {
    static Configuration conf;
    public HBase() throws Exception {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","10.141.209.224");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.master", "10.141.209.224:60000");
        conf.set("fs.defaultFS","hdfs://10.141.209.224:9000");
//        HBaseAdmin admin = new HBaseAdmin(conf);
    }

    public static void main(String[] args){
        try{
            HBase HBase = new HBase();
            HDFSClient hdfsClient = new HDFSClient("hdfs://10.141.209.224:9000");
//            byte[] txtArray = hdfsClient.readAsByteArray("hdfs://10.141.209.224:9000/xyl/datas-communication.txt");
//            TableName tableName = ("dbtest");
//            HBase.dataImport(TableName.valueOf("dbtest"), txtArray,"cf","c1","c2");
//            HBase.tableCreate("table1", new String[]{"called","calling-time"});
//            HBase.tableTrop("table1");
//            HBase.rowDelete("dbtest",new String[]{"rk001"});
            HBase.tableScan("test");
        }
        catch(IOException exception){
            exception.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void dataImport(TableName tableName, byte[] txtArray, String cf, String c1, String c2) throws IOException{
//        HTable table = new HTable(conf, tableName);
//        table.setAutoFlushTo(true);
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
//        HTableDescriptor table = admin.getTableDescriptor(tableName);

        Table table = connection.getTable(tableName);

        String Str = new String(txtArray);
        String[] txtLine = Str.split("\n");
        List<Put> lists = new ArrayList<Put>();
        for (String txt:txtLine){
            String rowkey = txt.split(" ")[0]+"-"+txt.split(" ")[2];
            Put put = new Put(rowkey.getBytes());
            put.addColumn(cf.getBytes(),c1.getBytes(),txt.split(" ")[1].getBytes());
            put.addColumn(cf.getBytes(),c2.getBytes(),txt.split(" ")[3].getBytes());
            table.put(put);
        }
        System.out.println("seed");
    }


    public void tableCreate(String tableNameString, String[] familyColumnNames) throws IOException{
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
//        HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
        TableName tableName = TableName.valueOf(tableNameString);
        if(admin.tableExists(tableName)){
            System.out.println(tableNameString+"has existed...");
        }
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
//        ColumnFamilyDescriptorBuilder familyDescriptorBuilder = ColumnFamilyDescriptorBuilder;
        Set<ColumnFamilyDescriptor> columnFamilyDescriptors = new HashSet<>();
        for(String familyName: familyColumnNames){
            columnFamilyDescriptors.add(ColumnFamilyDescriptorBuilder.of(familyName));
        }
        tableDescriptorBuilder.setColumnFamilies(columnFamilyDescriptors);
        admin.createTable(tableDescriptorBuilder.build());
        System.out.println("success!");
    }

    public void tableTrop(String tableNameString) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf(tableNameString);
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        System.out.println(tableName+" has been dropped!");
    }

    public void rowDelete(String tableNameString, String[] rowkeys) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        Table table = connection.getTable(TableName.valueOf(tableNameString));
//        HTable table = new HTable(conf, tableName);
//        List list = new ArrayList();
        for (String rowkey:rowkeys){
            Delete d1 = new Delete(rowkey.getBytes());
            table.delete(d1);
        }
//        list.add(d1);
        System.out.println("Some rows have been deleted!");
    }

    public void tableScan(String tableNameString) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        Table table =connection.getTable(TableName.valueOf(tableNameString));
        Scan scan = new Scan();
        ResultScanner resultScanners = table.getScanner(scan);
        for (Result resultScanner:resultScanners){
            System.out.println(resultScanner.toString());
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
