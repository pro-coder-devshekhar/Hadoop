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
public class student
{
public static class Map extends Mapper<LongWritable, Text, Text, Text>
{
public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
{
String name1;
name1 = context.getConfiguration().get("n1");
String[] line = value.toString().split(",");
if(line[1].equalsIgnoreCase(name1))
{
int total = Integer.parseInt(line[3]) + Integer.parseInt(line[4]) + Integer.parseInt(line[5]) + Integer.parseInt(line[6]) + Integer.parseInt(line[7]);
float avg = 0;
avg = total/5;
String marks = "\nStudent Total = " + total + "\nStudent Average = " + avg;
context.write(value,new Text(marks));
}
}
} public static void main(String[] args) throws Exception
{
Configuration conf = new Configuration();
conf.set("n1",args[2]); Job job = new Job(conf, "student-mark");
job.setJarByClass(student.class); job.setOutputKeyClass(Text.class); job.setOutputValueClass(Text.class); job.setMapperClass(Map.class); job.setInputFormatClass(TextInputFormat.class); job.setOutputFormatClass(TextOutputFormat.class); FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1])); job.waitForCompletion(true);
}
}