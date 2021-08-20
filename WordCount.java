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
public class WordCount{
public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
{
//private final static IntWritable one = new IntWritable(1); IntWritable one = new IntWritable(1);
//private Text word = new Text();
public void map(LongWritable key, Text value, Context context) throws IOException,
InterruptedException
{ String[] line = value.toString().split(",");
for(String lines:line)
{ context.write(new Text(lines),one);}
}
}
public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
{ int k=0;
String str="Output";
public void reduce(Text key, Iterable<IntWritable> values, Context context)
throws IOException, InterruptedException
{
int sum = 0;
for (IntWritable val : values)
{ sum += val.get();
}
context.write(new Text(key), new IntWritable(sum));
}
}
public static void main(String[] args) throws Exception
{ Configuration conf = new Configuration();
Job job = new Job(conf, "wordcount");
job.setJarByClass(WordCount.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(IntWritable.class);
job.setMapperClass(Map.class);
job.setReducerClass(Reduce.class);
job.setInputFormatClass(TextInputFormat.class); job.setOutputFormatClass(TextOutputFormat.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
job.waitForCompletion(true);
}
}