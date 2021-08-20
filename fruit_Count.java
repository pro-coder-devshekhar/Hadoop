package Dev_hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
public class fruit_Count {
	public static class Map extends Mapper<LongWritable,Text,Text,Text> { 
		public void map(LongWritable key, Text value, Context context)	throws IOException,InterruptedException	 
				{
				 String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
				 		String line=value.toString();
				 		String words[]=line.split(",");
				 		for(String s:words){
				 	context.write(new Text(s), new Text(fileName));
				 }
				}
		}
	 public static class ReduceJoinReducer extends Reducer <Text, Text, Text, Text>
	 {
		 public void reduce(Text key, Iterable<Text> values, Context context)
		 throws IOException, InterruptedException 
		 {
			HashMap m=new HashMap();
				int count=0;
				for(Text t:values){
				String str=t.toString();	 
			if(m!=null &&m.get(str)!=null){
			 	count=(int)m.get(str); 	 
			m.put(str, ++count);
			}
			else{	 
				m.put(str, 1);
			 }
			}
		context.write(key, new Text(m.toString()));
		}
	}
	 public static void main(String[] args) throws Exception {		 
			Configuration conf= new Configuration();
			Job job = new Job(conf,"inverted index");
			job.setJarByClass(fruit_Count.class);
			job.setMapperClass(Map.class);
			job.setReducerClass(ReduceJoinReducer.class);
			job.setOutputKeyClass(Text.class);
	   	    job.setOutputValueClass(Text.class);		 
	   	    job.setOutputKeyClass(Text.class);
	   	    job.setOutputValueClass(Text.class);
	   	    FileInputFormat.addInputPath(job, new Path(args[0]));
	   	    FileOutputFormat.setOutputPath(job, new Path(args[1]));	  
	   	    job.waitForCompletion(true);		
	 }
}
