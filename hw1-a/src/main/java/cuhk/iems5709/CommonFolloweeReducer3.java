package cuhk.iems5709;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class CommonFolloweeReducer3 extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String followerLast=new String();
        int countLast=0;
        for (Text value:values) {
            String[] strings=value.toString().split("-");
            String follower=strings[0];
            int count=Integer.parseInt(strings[1]);
            if (count>countLast){
                countLast=count;
                followerLast=follower;
            }
        }
        context.write(key,new Text(followerLast));
    }
}
