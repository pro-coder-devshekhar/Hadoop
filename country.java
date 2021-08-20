package Dev_hadoop;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*; public class country
{
public static class Map extends Mapper<LongWritable, Text, Text, Text>
{
public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
{
String[] line = value.toString().split(",");
try
{
context.write(new Text(line[0]+"\t"+line[1]), new Text(line[2]));
}
catch(Exception e){}
}
}
public static class parts extends Partitioner<Text,Text>
{
public int getPartition(Text key,Text value,int nr)
{
String a=value.toString();
if(a.equalsIgnoreCase("Cricket"))
return 0;
if(a.equalsIgnoreCase("Football"))
return 1;
else
return 2;
}
}
public static class Reduce extends Reducer<Text, Text, Text, Text>
{
int count=0;
public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
{
for(Text b:values)
{
context.write(key,b);
count=count+1;
}
}
public void cleanup(Context context)throws IOException, InterruptedException
{
context.write(new Text("\n Total count is :"),new Text(String.valueOf(count)));
}
}
public static void main(String[] args) throws Exception
{
Configuration conf = new Configuration();
Job job = new Job(conf, "country"); 
job.setJarByClass(country.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);
job.setMapperClass(Map.class);
job.setReducerClass(Reduce.class);
job.setInputFormatClass(TextInputFormat.class);
job.setOutputFormatClass(TextOutputFormat.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
job.setPartitionerClass(parts.class);
job.setNumReduceTasks(3);
job.waitForCompletion(true);
}
}