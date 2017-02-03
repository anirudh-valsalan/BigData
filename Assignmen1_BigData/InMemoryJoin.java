
import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
	import org.apache.hadoop.mapreduce.Mapper;
	import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InMemoryJoin extends Configured implements Tool{
	static String test="";
	static HashMap<String,String> myMap;
	
    
   static  HashSet<Integer> tempMap=new HashSet<>();
	    public static class FriendData
	            extends Mapper<Text, Text, Text, Text> {
	        
	    	StringBuilder friendData=new StringBuilder("[");
	    	String keyUser="";
	    	public void setup(Context context) throws IOException {
	            Configuration config = context.getConfiguration();
	         
	             
	             myMap = new HashMap<String,String>();
	 			String mybusinessdataPath = config.get("businessdata");
	 		
	 			Path pt=new Path("hdfs://cshadoop1"+mybusinessdataPath);//Location of file in HDFS
		        FileSystem fs = FileSystem.get(config);
		        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		        String line;
		        line=br.readLine();
		        while (line != null){
		        	String[] arr=line.split(",");
 		        	if(arr.length == 10){
 		        		String data=arr[1]+":"+arr[6];
 		            myMap.put(arr[0].trim(), data); 
 		        	}
		            line=br.readLine();
		        }	  
	        }
	       
	        int count=0;
	    
	        public void map(Text key, Text value, Context context)
	                throws IOException, InterruptedException {
	        	String line = value.toString();
	            String[] split = line.split(",");
	   keyUser=key.toString();         
	         
	          if(null!=myMap && !myMap.isEmpty()){
	        	  
	        for(String s:split)
	        {
	           if(myMap.containsKey(s)){
	        	   friendData=friendData.append(myMap.get(s)+",");
	        	   myMap.remove(s);
	        	  
	        	   //context.write(key, new Text(friendData));
	           }
	           }
	          }
	        }
	        protected void cleanup(Context context)
	                throws IOException,
	                       InterruptedException{
				
				friendData.setLength(friendData.length()-1);
				friendData.append("]");
				
				Text opVal = new Text(friendData.toString());
				Text opKey = new Text(keyUser);
				context.write(opKey, opVal);
			}
}
	    public static void main(String args[]) throws Exception {
	        // Standard Job setup procedure.
	    	 int res = ToolRunner.run(new Configuration(), new InMemoryJoin(), args);
	    	    System.exit(res);
	    	
	    }
	    public int run(String[] otherArgs) throws Exception {
			// TODO Auto-generated method stub
	    	Configuration conf = new Configuration();
			//String[] otherArgs = new GenericOptionsParser(conf, args0).getRemainingArgs();		// get all args
			if (otherArgs.length != 6) {
				System.err.println("Usage: InMemoryJoin <user1> <user2> <input> <out> <inmemory input> <output>");
				System.exit(2);
			}
   
			
			conf.set("userA", otherArgs[0]);
			conf.set("userB", otherArgs[1]);
			
			Job job = new Job(conf, "InlineArgument");
			job.setJarByClass(InMemoryJoin.class);
			
			
			 job.setMapperClass(MutualFriends.FriendsMapper.class);
		   //  job.setReducerClass(MutualFriends.FriendsReducer.class);
			 job.setNumReduceTasks(0);
			job.setOutputKeyClass(Text.class);

			job.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
			Path p=new Path(otherArgs[3]);
			FileOutputFormat.setOutputPath(job, p);
	
			int code = job.waitForCompletion(true)?0:1;
			
		 
		   
			Configuration conf1 = getConf();
			conf1.set("businessdata", otherArgs[4]);
				   	Job job2 = new Job(conf1, "InMemoryJoin");
				   	job2.setJarByClass(InMemoryJoin.class);
				   	job2.setInputFormatClass(KeyValueTextInputFormat.class);
					
					job2.setMapperClass(FriendData.class);
					//job2.setReducerClass(MapReduce.class);
					job2.setNumReduceTasks(0);
					job2.setOutputKeyClass(Text.class);
					job2.setOutputValueClass(Text.class);
					
					FileInputFormat.addInputPath(job2, p);
					FileOutputFormat.setOutputPath(job2, new Path(otherArgs[5]));
							
					// Execute job and grab exit code
					code = job2.waitForCompletion(true) ? 0 : 1;
			   
				
				//FileSystem.get(conf).delete(p, true);
				System.exit(code);
				return code;
		}
}