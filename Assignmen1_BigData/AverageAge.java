 import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.hash.Hash;



public class AverageAge 
{
		
	public static class FriendMapper extends Mapper<LongWritable, Text, LongWritable, Text> 
	{
		   LongWritable userId =new LongWritable();
	        Text friends=new Text();

		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			 String line[] = value.toString().split("\t");
	            userId.set(Long.parseLong(line[0]));
	            Text data=new Text();
	            data.set(userId.toString());
	          
	            if (line.length != 1)
	            {
	            	String mutualfriends = line[1];
	            	String outvalue=("U:" + mutualfriends.toString());
					context.write(userId, new Text(outvalue));
				}
			}
		}
	
	
	public static class DetailsMapper extends Mapper<LongWritable, Text, LongWritable, Text> 
	{
		private LongWritable outkey = new LongWritable();
		private Text outvalue = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			 String arr[] = value.toString().split(",");
	        	if(arr.length == 10){
	        		
	            outkey.set(Long.parseLong(arr[0]));
				
	            String[] cal=arr[9].toString().split("/");
			
				 Date now = new Date();
				    int nowMonth = now.getMonth()+1;
				    int nowYear = now.getYear()+1900;
				    int result = nowYear - Integer.parseInt(cal[2]);

				    if (Integer.parseInt(cal[0]) > nowMonth) {
				        result--;
				    }
				    else if (Integer.parseInt(cal[0]) == nowMonth) {
				        int nowDay = now.getDate();

				        if (Integer.parseInt(cal[1]) > nowDay) {
				            result--;
				        }
				    }
				    String data=arr[1]+","+new Integer(result).toString()+","+arr[3]+","+arr[4]+","+arr[5];
				outvalue.set("R:" + data);
						
				context.write(outkey, outvalue);
			}
		}

	}
	
	public static class JoinReducer extends Reducer<LongWritable, Text, Text, Text> 
	{
		private ArrayList<Text> listA = new ArrayList<Text>();
		private ArrayList<Text> listB = new ArrayList<Text>();
		HashMap<String, String> myMap=new HashMap<>();
		 public void setup(Context context) throws IOException {
	            Configuration config = context.getConfiguration();
	                      myMap = new HashMap<String,String>();
	 			String mybusinessdataPath = config.get("userdata");
	 			
	 			Path pt=new Path("hdfs://cshadoop1"+mybusinessdataPath);//Location of file in HDFS
		        FileSystem fs = FileSystem.get(config);
		        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		        String line;
		        line=br.readLine();
		        while (line != null){
		        	String[] arr=line.split(",");
		        	if(arr.length == 10){
		            myMap.put(arr[0].trim(), arr[1]+":"+arr[3]+":"+arr[9]); 
		        	}
		            line=br.readLine();
		        }	 
	        }
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			// Clear our lists
			listA.clear();
			listB.clear();
			
			for (Text val : values)
			{
				if (val.toString().charAt(0) == 'U')
				{
					listA.add(new Text(val.toString().substring(2)));
				} 		
				else if (val.toString().charAt(0) == 'R') 
				{
					listB.add(new Text(val.toString().substring(2)));
				}
			}//End_For
			Text C=new Text();
			float age=0;
			int count=0;
			float avgAge;
			String[] details = null;
		
			if(!listA.isEmpty() && !listB.isEmpty())
			{
				for(Text A : listA)
				{
					String frd[]=A.toString().split(",");
					
							for(int i=0;i<frd.length;i++)
							{
								if(myMap.containsKey(frd[i]))
								{
									String[] ageCalu=myMap.get(frd[i]).split(":");
									 Date now = new Date();
									    int nowMonth = now.getMonth()+1;
									    int nowYear = now.getYear()+1900;
									    String[] cal=ageCalu[2].toString().split("/");
									   int result = nowYear - Integer.parseInt(cal[2]);

									    if (Integer.parseInt(cal[0]) > nowMonth) {
									        result--;
									    }
									    else if (Integer.parseInt(cal[0]) == nowMonth) {
									        int nowDay = now.getDate();

									        if (Integer.parseInt(cal[1]) > nowDay) {
									            result--;
									        }
									    }
									    age+=result;
									    count++;
								}
							}
							avgAge=(float)(age/count);
							String SubDetails="";
							
							for(Text B:listB)
							{
								details=B.toString().split(",");
								SubDetails=B.toString()+","+new Text(new FloatWritable((float) avgAge).toString());
							}
							
							C.set(SubDetails);
					}
				
			}
			context.write(new Text(key.toString()),C);	
			}
		
	}
	public static class AverageAgeMapper extends Mapper<LongWritable, Text, AverageAgeCustom, Text> 
	{
		

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			
			String[] m=value.toString().split("\t");
			
			if(m.length==2){
			String line[] = m[1].split(",");
			
             context.write(new AverageAgeCustom(Float.parseFloat(m[0]),Float.parseFloat(line[5])), new Text(m[1].toString()));
			}		
		}
				
			}
	
		
	
	public static class AverageAgeCustom implements  WritableComparable<AverageAgeCustom>  {

	  	  private Float userId;
	  	  private Float age;

	  	  public Float getUserId() {
			return userId;
		}
		public void setUserId(Float userId) {
			this.userId = userId;
		}
		
		
		
		public Float getAge() {
			return age;
		}
		public void setAge(Float age) {
			this.age = age;
		}
		public AverageAgeCustom(Float user, Float age) {
				// TODO Auto-generated constructor stub
	  		  this.userId=user;
	  		  this.age=age;
			}
	  	  public AverageAgeCustom(){}

			@Override
	  	  public void readFields(DataInput in) throws IOException {
	  	    userId = in.readFloat();
	  	    age = in.readFloat();
	  	  }

	  	  @Override
	  	  public void write(DataOutput out) throws IOException {
	  	    out.writeFloat(userId);;
	  	    out.writeFloat(age);;
	  	  }
	  	
			@Override
			public int compareTo(AverageAgeCustom o) {
				// TODO Auto-generated method stub
				
		         
		        int result = userId.compareTo(o.userId);
		        if (result != 0) {
		            return result;
		        }
		        return this.age.compareTo(o.age);
		      
		    }
			@Override
			public String toString() {
			return userId.toString() + ":" + age.toString();
			}
			@Override
			public boolean equals(Object obj) {
			    if (obj == null) {
			        return false;
			    }
			    if (getClass() != obj.getClass()) {
			        return false;
			    }
			    final AverageAgeCustom other = (AverageAgeCustom) obj;
			    if (this.userId != other.userId && (this.userId == null || !this.userId.equals(other.userId))) {
			        return false;
			    }
			    if (this.age != other.age && (this.age == null || !this.age.equals(other.age))) {
			        return false;
			    }
			    return true;
			}
		
	}	
	
	
	
	public class AverageAgePartitioner extends Partitioner<AverageAgeCustom, Text>{
	    @Override
	    public int getPartition(AverageAgeCustom averageAge, Text nullWritable, int numPartitions) {
	        return averageAge.getAge().hashCode() % numPartitions;
	    }
	}
	
	
	public static class AverageAgeBasicCompKeySortComparator extends WritableComparator {

		  public AverageAgeBasicCompKeySortComparator() {
				super(AverageAgeCustom.class, true);
			}

			@Override
			public int compare(WritableComparable w1, WritableComparable w2) {
				AverageAgeCustom key1 = (AverageAgeCustom) w1;
				AverageAgeCustom key2 = (AverageAgeCustom) w2;

				int cmpResult = -1*key1.getAge().compareTo(key2.getAge());
				
				
				return cmpResult;
			}
		}
	public static class AverageAgeBasicGroupingComparator extends WritableComparator {
		  public AverageAgeBasicGroupingComparator() {
				super(AverageAgeCustom.class, true);
			}

			@Override
			public int compare(WritableComparable w1, WritableComparable w2) {
				AverageAgeCustom key1 = (AverageAgeCustom) w1;
				AverageAgeCustom key2 = (AverageAgeCustom) w2;
				return -1*key1.getAge().compareTo(key2.getAge());
			}
		}
	public static class AverageAgeReducer extends Reducer<AverageAgeCustom, Text, Text, Text> 
	{
		TreeMap<String,String> output=new TreeMap<String, String>();        
	
		         
		public void reduce(AverageAgeCustom key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			for(Text t:values)
			{
				if(output.size()<20)
				{
					output.put(key.userId.toString(), t.toString());
					 context.write(new Text(t.toString().split(",")[0]), new Text(t));
				}
			}
		}
	}

	public static void main(String[] args) throws Exception 
	{

		Path outputDirIntermediate1 = new Path(args[3] + "_int1");
		Path outputDirIntermediate2 = new Path(args[4] + "_int2");
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		conf.set("userdata",otherArgs[0]);
		// get all args
		if (otherArgs.length != 5)
		{
			System.err.println("Usage: JoinExample <inmemory input> <input > <input> <intermediate output> <output>");
			System.exit(2);
		}
		
		Job job = new Job (conf, "join1 ");
		job.setJarByClass(AverageAge.class);
		job.setReducerClass(JoinReducer.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[1]),TextInputFormat.class, FriendMapper.class );
		MultipleInputs.addInputPath(job, new Path(otherArgs[2]),TextInputFormat.class, DetailsMapper.class );

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
	
		FileOutputFormat.setOutputPath(job,outputDirIntermediate1);
		
	    int code = job.waitForCompletion(true)?0:1;
	    Job job1 = new Job(new Configuration(), "join2");
		job1.setJarByClass(AverageAge.class);
		FileInputFormat.addInputPath(job1, new Path(args[3] + "_int1"));		
		job1.setMapOutputKeyClass(AverageAgeCustom.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setPartitionerClass(AverageAgePartitioner.class);
		 job1.setMapperClass(AverageAgeMapper.class);
		job1.setSortComparatorClass(AverageAgeBasicCompKeySortComparator.class);
		job1.setGroupingComparatorClass(AverageAgeBasicGroupingComparator.class);
		job1.setReducerClass(AverageAgeReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job1,outputDirIntermediate2);
		code = job1.waitForCompletion(true) ? 0 : 1;
	
			
	}
}

