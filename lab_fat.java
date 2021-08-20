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

public class lab_fat 
{
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		int c;
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{ 
			String line = value.toString();
			String[] letters = line.split(",");
			for (String letter : letters) 
			{
				if(Vowel(letter)==0)
					context.write(new Text(letter + "\t" + letter.length()), new IntWritable(1));
			}
		}
		
		public int Vowel(String line) 
		{
			int c=0;
			for (int i=0 ; i<line.length(); i++)
			{
			    char ch = line.charAt(i);
			    if (ch == 'a' || ch == 'e' || ch == 'i' || ch == 'o' || ch == 'u') 
				    c++;
			}
			return c;
		}
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		int k=0;
		String str="Total no. of words with consonants = ";
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int sum = 0;
	        for (IntWritable val : values) 
	        {
	            sum += val.get();
	        }
	        k = k + sum;
	        context.write(new Text(key), new IntWritable(sum));
		}
	
		public void cleanup(Context context)throws IOException, InterruptedException
		{
			context.write(new Text(str), new IntWritable(k));
		}
	}
	
	public static void main(String[] args) throws Exception
	{ 
		Configuration conf = new Configuration();
		Job job = new Job(conf, "fat"); 
		job.setJarByClass(lab_fat.class); 
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class); 
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}
