package cuhk.iems5709;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TopSimilarReducer1 extends Reducer<Text,Text,Text,Text> {
    Text followers=new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        StringBuffer stringBuffer = new StringBuffer();
        for (Text value:values) {
            stringBuffer.append(value.toString()+",");
        }
        stringBuffer.deleteCharAt(stringBuffer.length()-1);
        followers.set(stringBuffer.toString());
        context.write(key,followers);
    }
}
