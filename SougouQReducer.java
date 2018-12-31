package allSougouQ;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

public class SougouQReducer extends Reducer<Text, Text, Text, Text> {
    HashMap<String, String> map = new HashMap<>();
    @Override
    protected void reduce(Text key3, Iterable<Text> values3, Context context) throws IOException, InterruptedException {
        String data = "";
        for (Text v : values3) {
            data += v.toString() + ";";
        }
        map.put(key3.toString(), data);

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Iterator iterator = map.entrySet().iterator();
        String result = "";
        while (iterator.hasNext()){
            HashMap.Entry entry = (HashMap.Entry) iterator.next();
            try {
                Object key = entry.getKey();
                Object value = entry.getValue();
                String[] each_user_info = value.toString().split(";");
                for (String ecah_info : each_user_info) {
                    String[] user_clicks = ecah_info.split(",");
                    if (user_clicks.length != 3) return;
                    String name = user_clicks[0];
                    String url = user_clicks[2];
                    result += name + ":" + url + ";";
                }
                context.write(new Text(key.toString()), new Text(result));
            }catch (Exception e){
                e.printStackTrace();
            }

        }
    }
}