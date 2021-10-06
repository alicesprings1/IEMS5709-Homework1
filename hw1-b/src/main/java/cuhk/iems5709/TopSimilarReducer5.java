package cuhk.iems5709;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class TopSimilarReducer5 extends Reducer<Text,Text,Text, FloatWritable> {
    float numberOfTotalFollowees,numberOfCommonFollowees,similarity;
    FloatWritable sim=new FloatWritable();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
        ArrayList<Integer> list=new ArrayList<>();
        for(Text value:values){
            list.add(Integer.parseInt(value.toString()));
        }
        if (list.size()==2){
            int i=list.get(0);
            int j=list.get(1);
            numberOfTotalFollowees=i>j?i:j;
            numberOfCommonFollowees=i<j?i:j;
            similarity=numberOfCommonFollowees/(numberOfTotalFollowees-numberOfCommonFollowees);
            sim.set(similarity);
            context.write(key,sim);
        }
    }
}
