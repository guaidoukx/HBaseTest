package reader;

import reader.DownThread;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.RandomAccessFile;

public class MultiDown {
    public static void main(String[] args) {
        final int DOWN_THREAD_NUM = 4;
        String filePath = "/Users/xiangyali/Desktop/data400.txt";
        final String OUT_FILE_NAME = filePath;
        InputStream[] isArr = new InputStream[DOWN_THREAD_NUM];
        RandomAccessFile[] outArr = new RandomAccessFile[DOWN_THREAD_NUM];
        try {
            isArr[0] = new FileInputStream(filePath);
            long fileLen = getFileLength(new File(filePath));
            System.out.println("文件的大小" + fileLen);
            //以输出文件名创建第一个RandomAccessFile输出流
            outArr[0] = new RandomAccessFile(OUT_FILE_NAME, "rw");
            //创建一个与文件相同大小的空文件
            for (int i = 0; i < fileLen; i++) {
                outArr[0].write(0);
            }
            //每线程应该读取的字节数
            long numPerThred = fileLen / DOWN_THREAD_NUM;
            //整个文件整除后剩下的余数
            long left = fileLen % DOWN_THREAD_NUM;
            for (int i = 0; i < DOWN_THREAD_NUM; i++) {
            //为每个线程打开一个输入流、一个RandomAccessFile对象，
            //让每个线程分别负责读取文件的不同部分。
                if (i != 0) {
                    isArr[i] = new FileInputStream(filePath);
                    //以指定输出文件创建多个RandomAccessFile对象
                    outArr[i] = new RandomAccessFile(OUT_FILE_NAME, "rw");
                }
                if (i == DOWN_THREAD_NUM - 1) {
                    //最后一个线程读取指定numPerThread+left个字节
                    new DownThread(i * numPerThred, (i + 1) * numPerThred
                            + left, isArr[i], outArr[i]).start();
                } else {
                    //每个线程负责读取一定的numPerThred个字节
                    new DownThread(i * numPerThred, (i + 1) * numPerThred,
                            isArr[i], outArr[i]).start();
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    public static long getFileLength(File file) {
        long length = 0;
        //获取文件的长度
        long size = file.length();
        length = size;
        return length;
    }
}
