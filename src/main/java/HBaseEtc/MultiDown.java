package HBaseEtc;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.RandomAccessFile;

public class MultiDown {
    public static void main(String[] args) {
        final int DOWN_THREAD_NUM = 4;
        final String OUT_FILE_NAME = "d:/copy????.rmvb";
        InputStream[] isArr = new InputStream[DOWN_THREAD_NUM];
        RandomAccessFile[] outArr = new RandomAccessFile[DOWN_THREAD_NUM];
        try {
            isArr[0] = new FileInputStream("d:/????.rmvb");
            long fileLen = getFileLength(new File("d:/????.rmvb"));
            System.out.println("?????" + fileLen);
//???????????RandomAccessFile???
            outArr[0] = new RandomAccessFile(OUT_FILE_NAME, "rw");
//???????????????
            for (int i = 0; i < fileLen; i++) {
                outArr[0].write(0);
            }
//???????????
            long numPerThred = fileLen / DOWN_THREAD_NUM;
//????????????
            long left = fileLen % DOWN_THREAD_NUM;
            for (int i = 0; i < DOWN_THREAD_NUM; i++) {
//???????????????RandomAccessFile???
//???????????????????
                if (i != 0) {
                    isArr[i] = new FileInputStream("d:/????.rmvb");
//???????????RandomAccessFile??
                    outArr[i] = new RandomAccessFile(OUT_FILE_NAME, "rw");
                }
                if (i == DOWN_THREAD_NUM - 1) {
//??????????numPerThred+left???
                    new DownThread(i * numPerThred, (i + 1) * numPerThred
                            + left, isArr[i], outArr[i]).start();
                } else {
//???????????numPerThred???
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
//???????
        long size = file.length();
        length = size;
        return length;
    }

}
