package allSougouQ;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SouGouQMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key1, Text value1, Context context) throws IOException, InterruptedException {
        String data= new String(value1.getBytes(), 0, value1.getLength(), "GBK");
        String[] infos = data.split("\t");
        if (infos.length != 4) return;
        String user = infos[0];
        String user_info = infos[1] + "," + infos[2] + "," + infos[3];
        context.write(new Text(user), new Text(user_info));
    }
}
