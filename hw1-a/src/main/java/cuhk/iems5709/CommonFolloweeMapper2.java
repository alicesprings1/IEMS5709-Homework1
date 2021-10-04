package cuhk.iems5709;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CommonFolloweeMapper2 extends Mapper<Text,Text,Text, IntWritable> {
    IntWritable v=new IntWritable(1);
    @Override
    protected void map(Text key, Text value, Mapper<Text, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String[] followers=value.toString().split(",");
        for (int i = 0; i < followers.length; i++) {
            for (int j = i+1; j < followers.length; j++) {
                context.write(new Text(followers[i]+"-"+followers[j]),v);
                context.write(new Text(followers[j]+"-"+followers[i]),v);
            }
        }
    }
}
