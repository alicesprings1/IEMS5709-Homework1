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

public class CommonFolloweeDriver3 {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR,"\t");
        Job job = Job.getInstance(conf);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setJarByClass(CommonFolloweeDriver3.class);
        job.setMapperClass(CommonFolloweeMapper3.class);
        job.setReducerClass(CommonFolloweeReducer3.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        FileOutputFormat.setOutputPath(job,new Path("/user/s1155164941/output-"+System.currentTimeMillis()));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
