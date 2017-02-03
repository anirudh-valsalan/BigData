import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
 
public class FriendRecommendation {
 
    public static class Assignment1_Q1_Map extends Mapper<LongWritable, Text, LongWritable, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String friendList = value.toString();
            String[] friendData = friendList.split("\t");
            LongWritable userOneKey = new LongWritable();
            Text userOneValue = new Text();
            if (friendData.length == 2) {
                LongWritable user = new LongWritable(Long.parseLong(friendData[0]));
                String[] friendsListId = friendData[1].split(",");
                String userOne;
               
                String userTwo;
                LongWritable userTwoKey = new LongWritable();
                Text friend2Value = new Text();
                for (int i = 0; i < friendsListId.length; i++) {
                    userOne = friendsListId[i];
                    userOneValue.set(userOne+",Friend");
                    context.write(user, userOneValue);   
                    userOneKey.set(Integer.parseInt(userOne));
                    userOneValue.set(userOne+",Recommend");
                    for (int j = i+1; j < friendsListId.length; j++) {
                        userTwo = friendsListId[j];
                        userTwoKey.set(Integer.parseInt(userTwo));
                        friend2Value.set(userTwo+",Recommend" );
                        context.write(userOneKey, friend2Value);   
                        context.write(userTwoKey, userOneValue);  
                    }
                }
            }
        }
    } 
 
    public static class Assignment1_Q1_Reduce extends Reducer<LongWritable, Text, LongWritable, Text> {
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] value;
            HashMap<String, Integer> hash = new HashMap<String, Integer>();
            for (Text val : values) {
                value = (val.toString()).split(",");
                if (value[1].equals("Friend")) { 
                    hash.put(value[0], -1);
                } else if (value[1].equals("Recommend")) {  
                    if (hash.containsKey(value[0])) {
                        if (hash.get(value[0]) != -1) {
                            hash.put(value[0], hash.get(value[0]) + 1);
                        }
                    } else {
                        hash.put(value[0], 1);
                    }
                }
            }
            
           
            ArrayList<Entry<String, Integer>> list = new ArrayList<Entry<String, Integer>>();
            for (Entry<String, Integer> entry : hash.entrySet()) {
                if (entry.getValue() != -1) {   
                    list.add(entry);
                }
            }
            
            
            Collections.sort(list, new Comparator<Entry<String, Integer>>() {
             
                public int compare(Entry<String, Integer> firstEntry, Entry<String, Integer> secondEntry) {
                    Integer firstValue = firstEntry.getValue();
                    Integer secondValue = secondEntry.getValue();
                    
                    if (firstValue > secondValue) {
                        return -1;
                    } else if (firstValue.equals(secondValue) && Integer.parseInt(firstEntry.getKey()) < Integer.parseInt(secondEntry.getKey())) {
                        return -1;
                    } else {
                        return 1;
                    }
                }
            });
            int topTen = 10; 
            if (topTen < 1) {
                context.write(key, new Text(StringUtils.join(",", list)));
            } else {
                 ArrayList<String> top = new ArrayList<String>();
                for (int i = 0; i < Math.min(topTen, list.size()); i++) {
                    top.add(list.get(i).getKey());
                }
                context.write(key, new Text(StringUtils.join(",", top)));
            }
        }
    }
 
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
 
        Job job = new Job(conf, "Friend Recommendation Q1");
        job.setJarByClass(FriendRecommendation.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
 
        job.setMapperClass(Assignment1_Q1_Map.class);
        job.setReducerClass(Assignment1_Q1_Reduce.class);
 
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
        job.waitForCompletion(true);
    }
}