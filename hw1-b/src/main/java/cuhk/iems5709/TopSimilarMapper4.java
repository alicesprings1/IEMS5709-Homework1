package cuhk.iems5709;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TopSimilarMapper4 extends Mapper<Text, Text,Text,IntWritable> {
    Map<String,Integer> map=new HashMap<String,Integer>();
    Text followerPair1=new Text();
    Text followerPair2=new Text();
    IntWritable numberOfTotalFollowees=new IntWritable();
    @Override
    protected void map(Text key, Text value, Mapper<Text, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        map.put(key.toString(),Integer.parseInt(value.toString()));
    }

    @Override
    protected void cleanup(Mapper<Text, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        List<Map.Entry<String,Integer>> list=new ArrayList<Map.Entry<String,Integer>>(map.entrySet());
        for (int i = 0; i < list.size(); i++) {
            for (int j = i+1; j < list.size(); j++) {
                followerPair1.set(list.get(i).getKey()+":"+list.get(j).getKey());
                followerPair2.set(list.get(j).getKey()+":"+list.get(i).getKey());
                numberOfTotalFollowees.set(list.get(i).getValue()+list.get(j).getValue());
                context.write(followerPair1,numberOfTotalFollowees);
                context.write(followerPair2,numberOfTotalFollowees);
            }
        }
    }
}
