package Dev_hadoop;

import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
public class Customer
{
public static class custmapper extends Mapper<LongWritable, Text, Text, Text>
{
public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
{
String[] line = value.toString().split(",");
context.write(new Text(line[0]), new Text("cust"+","+line[1]));
}
}
public static class transmapper extends Mapper<LongWritable,Text,Text,Text>
{
public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
{
String[] line = value.toString().split(",");
context.write(new Text(line[0]), new Text("trans"+","+line[1]));
}
}
public static class jreducer extends Reducer<Text,Text,Text,Text>
{
String st1;
public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException
{
int c=0,amt=0;
for(Text val:values)
{
String[] line = val.toString().split(",");
if (line[0].equals("trans"))
{
c++; amt+= Integer.parseInt(line[1]);
}
else if (line[0].equals("cust"))
{
st1 = line[1];
}
}
context.write(new Text(st1), new Text(c+","+amt));
}
}
public static void main(String[] args) throws Exception
{
Configuration conf = new Configuration();
Job job = new Job(conf, "customer");
job.setJarByClass(Customer.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);
job.setReducerClass(jreducer.class);
job.setInputFormatClass(TextInputFormat.class);
job.setOutputFormatClass(TextOutputFormat.class);
MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class, custmapper.class);
MultipleInputs.addInputPath(job,new Path(args[1]), TextInputFormat.class, transmapper.class);
FileOutputFormat.setOutputPath(job, new Path(args[2]));
job.waitForCompletion(true);
}
}