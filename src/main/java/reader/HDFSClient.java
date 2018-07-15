package reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.lf5.util.StreamUtils;
import org.omg.Messaging.SYNC_WITH_TRANSPORT;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class HDFSClient {
    private FileSystem fileSystem;

    public HDFSClient() throws IOException {
        org.apache.hadoop.conf.Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://10.141.209.224:9000");
        fileSystem = FileSystem.newInstance(conf);
        System.out.println("HDFS has been attached !");
    }

    public byte[] readAsByteArray(String filePath) throws IOException {
        FSDataInputStream hdfsInStream = this.fileSystem.open(new Path(filePath));
        byte[] readResult = StreamUtils.getBytes(hdfsInStream);
        hdfsInStream.close();
        return readResult;
    }

    public InputStream readAsInputStream(String filePath) throws IOException{
        FSDataInputStream hdfsInStream = fileSystem.open(new Path(filePath));
        return hdfsInStream;
    }

    public List<String> listFileNames(String directoryPath) throws IOException {
        List<String> toReturn = new ArrayList<String>();
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path(directoryPath));
        for (FileStatus fileStatus : fileStatuses) {
            toReturn.add(fileStatus.getPath().toString());
        }
        return toReturn;
    }


}
