package Dev_hadoop;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
public class salary
{
public enum ct
{
cnt,nt
};
public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
{
public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
{
String[] line = value.toString().split(",");
String name = line[0];
int exp = Integer.parseInt(line[1]);
String des = line[2];
int sal = Integer.parseInt(line[3]);
String k = name + " " + des;
if(exp > 10)
{
context.getCounter(ct.cnt).increment(1);
context.write(new Text(k),new IntWritable(sal));
}
if(sal > 30000)
{
context.getCounter(ct.nt).increment(1);
}
}
}
public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
{
public void reduce(Text key, IntWritable value, Context context) throws IOException, InterruptedException
{
context.write(key,value);
}
}
public static void main(String[] args) throws Exception
{
Configuration conf = new Configuration();
Job job = new Job(conf, "salary");
job.setJarByClass(salary.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(IntWritable.class);
job.setMapperClass(Map.class);
job.setReducerClass(Reduce.class);
job.setInputFormatClass(TextInputFormat.class); job.setOutputFormatClass(TextOutputFormat.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
job.waitForCompletion(true);
Counters cn=job.getCounters();
cn.findCounter(ct.cnt).getValue();
cn.findCounter(ct.nt).getValue();
}
}