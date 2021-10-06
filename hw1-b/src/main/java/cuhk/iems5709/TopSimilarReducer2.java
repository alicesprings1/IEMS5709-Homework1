package cuhk.iems5709;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;


public class TopSimilarReducer2 extends Reducer<Text,Text,Text,Text> {
    Text numberOfCommonFollowees=new Text();
    Text commonFollowees=new Text();
    private MultipleOutputs mos;

    @Override
    protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        mos=new MultipleOutputs(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        StringBuffer stringBuffer = new StringBuffer();
        int sum=0;
        for (Text value:values) {
            stringBuffer.append(value.toString()+",");
            sum+=1;
        }
        commonFollowees.set(stringBuffer.deleteCharAt(stringBuffer.length()-1).toString());
        numberOfCommonFollowees.set(Integer.toString(sum));
        mos.write("commonFollowees",key,commonFollowees);
        mos.write("numberOfCommonFollowees",key,numberOfCommonFollowees);
    }

    @Override
    protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        mos.close();
    }
}
