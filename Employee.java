package Dev_hadoop;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Employee {
	
	public static class Map extends Mapper<LongWritable, Text,NullWritable,Text> 
	{
	private TreeMap<Integer, Text> salary = new TreeMap<Integer, Text>();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		String[] words = value.toString().split(",");
		int Salary=Integer.parseInt(words[3]);
	        salary.put(Salary,new Text(value));

	         if (salary.size() > 10) {
	         salary.remove(salary.firstKey());
	        }      
	}
	protected void cleanup(Context context) throws IOException, InterruptedException 
	{   	        for ( Text name : salary.values() ) {
	    	           context.write(NullWritable.get(), name);
	    	        }
	    	    }
	}
	public static class Reduce extends Reducer<NullWritable, Text, NullWritable, Text> 
	{ 	    
	public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


	
		  	        for (Text t : values) {
		        context.write(NullWritable.get(), t);
		   	        }
		  	    }
	}	  	
	 public static void main(String[] args) throws Exception {	
		 
			Configuration conf= new Configuration();
			Job job = new Job(conf,"topk");
			job.setJarByClass(Employee.class);
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			job.setNumReduceTasks(1); 
			job.setInputFormatClass(TextInputFormat.class); 
			job.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.waitForCompletion(true);
			
	 }

}

