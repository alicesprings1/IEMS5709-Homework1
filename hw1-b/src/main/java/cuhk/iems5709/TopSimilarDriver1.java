package cuhk.iems5709;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TopSimilarDriver1 {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR," ");
        Job job = Job.getInstance(conf);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setJarByClass(TopSimilarDriver1.class);
        job.setMapperClass(TopSimilarMapper1.class);
        job.setReducerClass(TopSimilarReducer1.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
//        FileInputFormat.setInputPaths(job,new Path(args[0]));
//        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        FileInputFormat.addInputPath(job,new Path("D:\\study\\hadoop\\homework1\\src\\test\\small_relation"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\study\\hadoop\\homework1\\src\\test\\output-"+System.currentTimeMillis()));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
