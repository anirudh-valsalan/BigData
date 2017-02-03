	import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
	import org.apache.hadoop.mapreduce.Mapper;
	import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
	import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class MutualFriends {

	ArrayList<Text> quadgram=new ArrayList<Text>;
	    public static class FriendsMapper
	            extends Mapper<LongWritable, Text, Text, Text> {
	    
	       
	      
	    
	        public void map(LongWritable key, Text value, Context context)
	                throws IOException, InterruptedException {
	        	String line = value.toString();
	            String[] split = line.split("\t");
	            String syscall = split[6];
	          
	            if(quadgram.size()<4)
				{
					quadgram.add(new Text(syscall));
				}
	            if(quadgram.size==4)
				{
					context.write("filename1"+new Text(StringUtils.join(",", quadgram), 1);
					quadgram.clear();
				}
	           
	    }

	   }
	

	    public static void main(String args[]) throws Exception {
	       
	    	Configuration conf = new Configuration();
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();		
			System.out.println(Arrays.toString(otherArgs));
			
		
			
			
			Job job = new Job(conf, "MutualFriends");
			
			job.setJarByClass(Check.class);
	
			
			 job.setMapperClass(FriendsMapper.class);
		       
			 job.setNumReduceTasks(0);
			job.setOutputKeyClass(Text.class);

			job.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	
			boolean sucess = job.waitForCompletion(true);
			
		}
}