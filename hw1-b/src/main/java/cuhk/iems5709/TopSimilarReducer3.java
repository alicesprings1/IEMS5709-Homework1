package cuhk.iems5709;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TopSimilarReducer3 extends Reducer<Text,Text,Text, IntWritable> {
    IntWritable count=new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int sum=0;
        for (Text value: values) {
            sum+=1;
        }
        count.set(sum);
        context.write(key,count);
    }
}
