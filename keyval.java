package Dev_hadoop;

import java.io.IOException; import java.util.*;
import org.apache.hadoop.fs.Path; import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
public class keyval
{
public static class Map extends Mapper<Text, Text,Text, Text>
{
String c="Dev";
public void map(Text key, Text value, Context context) throws IOException, InterruptedException
{
String line=key.toString();
if(c.equalsIgnoreCase(line))
context.write(key,value);
}
public static class Reduce extends Reducer<Text,Text,Text,Text>
{
public void reduce(Text key,Text value,Context context) throws IOException, InterruptedException
{
context.write(key, value);
}
}
}
public static void main(String[] args) throws Exception
{
Configuration conf = new Configuration();
conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
Job job = new Job(conf, "inputsplit");
job.setJarByClass(keyval.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);
job.setMapperClass(Map.class);
job.setInputFormatClass(KeyValueTextInputFormat.class);
job.setOutputFormatClass(TextOutputFormat.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
job.waitForCompletion(true);
}
}