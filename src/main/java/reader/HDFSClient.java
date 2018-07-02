package reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.lf5.util.StreamUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HDFSClient {
    private String fileSystemPath;
    private FileSystem fileSystem;

    public HDFSClient(String fileSystemPath) throws IOException {
        this.fileSystemPath = fileSystemPath;
        org.apache.hadoop.conf.Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://10.141.209.224:9000");
        fileSystem = FileSystem.newInstance(conf);
    }

    public byte[] readAsByteArray(String filePath) throws IOException {
        FSDataInputStream hdfsInStream = fileSystem.open(new Path(filePath));
        byte[] readResult = StreamUtils.getBytes(hdfsInStream);
        hdfsInStream.close();
        return readResult;
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
