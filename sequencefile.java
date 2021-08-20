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
public class sequencefile {
public static class Map extends Mapper<Text,Text,Text, Text>
{
public void map(Text key, Text value, Context context) throws IOException, InterruptedException
{
context.write(key, value);
}
}
public static void main(String[] args) throws Exception
{
Configuration conf = new Configuration();
Job job = new Job(conf, "inputsplit");
job.setJarByClass(sequencefile.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);
job.setMapperClass(Map.class);
job.setInputFormatClass(KeyValueTextInputFormat.class);
job.setOutputFormatClass(SequenceFileOutputFormat.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
job.waitForCompletion(true);
}
public Text LongWritable(LongWritable key)
{
return null;
}
}