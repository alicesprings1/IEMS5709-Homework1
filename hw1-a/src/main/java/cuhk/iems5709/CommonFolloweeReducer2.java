package cuhk.iems5709;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CommonFolloweeReducer2 extends Reducer<Text, IntWritable,Text,Text> {
    Text count= new Text();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, Text>.Context context) throws IOException, InterruptedException {
        int sum=0;
        for (IntWritable value:values) {
            sum+=value.get();
        }
        count.set(String.valueOf(sum));
        context.write(key,count);
    }
}
