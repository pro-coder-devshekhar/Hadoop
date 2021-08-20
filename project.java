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

public class Project 
{
	public static class Map extends Mapper<LongWritable, Text, Text, Text>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] line = value.toString().split(",");
			String exercise_segment = line[1];
			String timestamp_ip = line[11] + "\t" + line[3];
			context.write(new Text(timestamp_ip),new Text(exercise_segment));
		}
	}
	public static class dpart extends Partitioner<Text,Text>
	{
		public int getPartition(Text key,Text value,int nr)
		{
			String a=value.toString();
			if(a.equalsIgnoreCase("global"))
				return 0;
			else
				return 1;
		}
	}
	public static class Reduce extends Reducer<Text, Text, Text, Text> 
	{
		public void reduce(Text key, Text value, Context context) throws IOException, InterruptedException 
		{
			context.write(key, value);
		}
	}
	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Project");
		job.setJarByClass(Project.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(Map.class);
		job.setPartitionerClass(dpart.class);
		job.setNumReduceTasks(2);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}
