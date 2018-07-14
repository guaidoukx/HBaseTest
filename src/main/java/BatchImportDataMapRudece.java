import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
//import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class BatchImportDataMapRudece extends Configured implements Tool {


    public static void main(String[] args) throws Exception {
        BatchImportDataMapRudece im = new BatchImportDataMapRudece();
        im.run(args);
    }

    public int run(String[] strings) throws Exception {
        String jobName = "insert data into hbase";
        String outputTable = "OutTable";
        String inputPath = "/usr/mapreduce/input";
        String outputPath = "usr/maprduce/output";
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, jobName);

        job.setJarByClass(BatchImportDataMapRudece.class);

        job.setMapperClass(InsertMap.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(inputPath));
        FileOutputFormat.setOutputPath(job,new Path(outputPath));


//        TableMapReduceUtil.initTableReducerJob(outputTable,null,job);
        job.setNumReduceTasks(0);
        return job.waitForCompletion(true) ? 0 : 1;
    }


    public class InsertMap extends Mapper<Writable, Text, ImmutableBytesWritable, Put> {
        @Override
        protected void map(Writable key, Text value, Context context) {
            try {

                String line = value.toString();
                String[] items = line.split(",", -1);
                ImmutableBytesWritable outkey = new ImmutableBytesWritable(items[0].getBytes());
                String rk = items[0];//rowkey??
                Put put = new Put(rk.getBytes());
//                put.add("f1".getBytes(), "c1".getBytes(), items[0].getBytes());
//                put.add("f1".getBytes(), "c2".getBytes(), items[1].getBytes());
                context.write(outkey, put);
            } catch (Exception e) {


            }
        }

    }


}



