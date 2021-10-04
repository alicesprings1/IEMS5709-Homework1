package cuhk.iems5709;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CommonFolloweeMapper3 extends Mapper<Text, Text,Text,Text> {
    Text k,v;
    @Override
    protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] followers=key.toString().split("-");
        k.set(followers[0]);
        v.set(followers[1]+"-"+value);
        context.write(k,v);
        context.write(k,value);
    }

//    @Override
//    protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
//        super.map(key, value, context);
//    }
}
