package cuhk.iems5709;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TopSimilarMapper2 extends Mapper<Text,Text,Text,Text> {
    Text followersPair1=new Text();
    Text followersPair2=new Text();
    @Override
    protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] followers=value.toString().split(",");
        for (int i = 0; i < followers.length; i++) {
            for (int j = i+1; j < followers.length; j++) {
                followersPair1.set(followers[i]+":"+followers[j]);
                followersPair2.set(followers[j]+":"+followers[i]);
                context.write(followersPair1,key);
                context.write(followersPair2,key);
            }
        }
    }
}
