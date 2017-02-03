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

	static  String user1 ="";
	static  String user2 ="";
	static   HashSet<Integer> h1 = new HashSet<Integer>();
	static    HashSet<Integer> h2 = new HashSet<Integer>();
	    public static class FriendsMapper
	            extends Mapper<LongWritable, Text, Text, Text> {
	    
	        Long inputUser=new Long(-1L);
  	      boolean first=false;
  	        public void setup(Context context) {
  	        	
	            Configuration config = context.getConfiguration();
	             user1 = config.get("userA");
	             user2 = config.get("userB");
	        }
	      
	    
	        public void map(LongWritable key, Text value, Context context)
	                throws IOException, InterruptedException {
	        	String line = value.toString();
	            String[] split = line.split("\t");
	            String subject = split[0];
	          
	            inputUser=Long.parseLong(subject);
	            
	            if(split.length==2)
	            {
	            			String others = split[1];
	      	            	
	      	            	if((subject.equals(user1)) || (subject.equals(user2)))
	      	            	{
	      	            	 
	      	  	             if(null !=others)
	      	  	             
	      	  	             {
	      	  				     String[] s=others.split(",");
	      	  				 if(!first){    
	      	  					 first=true;
	      	  				     for(int i=0;i<s.length;i++)
	      	  				     {
	      	  				     	h1.add(Integer.parseInt(s[i]));
	      	  				     }
	      	  				 }
	      	  				 else
	      	  				 {
	      	  			       	for(int i=0;i<s.length;i++)
	      	  	             	{
	      	  	             		if(h1.contains(Integer.parseInt(s[i])))
	      	  	             		{
	      	  	             			h2.add(Integer.parseInt(s[i]));
	      	  	             		}
	      	  	             	}
	      	  				 }
	      	  	             }
	            }
	        }
	       
	    }
	        protected void cleanup(Context context)
	                throws IOException,
	                       InterruptedException{
				
				
				
				Text opKey = new Text(user1+","+user2);
				Text opVal = new Text(StringUtils.join(",", h2));
				context.write(opKey, opVal);
			}
	    }

	   
	

	    public static void main(String args[]) throws Exception {
	        // Standard Job setup procedure.
	    	Configuration conf = new Configuration();
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();		// get all args
			System.out.println(Arrays.toString(otherArgs));
			
		//	DistributedCache.addCacheFile(new URI("hdfs://cshadoop1"+ otherArgs[1]), conf);       
			
			conf.set("userA", otherArgs[0]);
			conf.set("userB", otherArgs[1]);
			
			
			Job job = new Job(conf, "MutualFriends");
			
			job.setJarByClass(MutualFriends.class);
	
			
			 job.setMapperClass(FriendsMapper.class);
		       
			 job.setNumReduceTasks(0);
			job.setOutputKeyClass(Text.class);

			job.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
	
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
	
			boolean sucess = job.waitForCompletion(true);
			
		}
}