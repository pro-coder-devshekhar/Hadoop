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
public class titanic {
public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
{
IntWritable one = new IntWritable(1);
public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
String[] line = value.toString().split(",");
int survive =Integer.parseInt(line[1]);
if(survive==1){
try{
context.write(new Text(line[4]), new IntWritable(Integer.parseInt(line[5])));
}
catch(Exception e){
context.write(new Text(line[4]), new IntWritable(Integer.parseInt("0")));
}
}
}
}
public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
int max=0;
Text gender = new Text();
Text gen = new Text();
IntWritable count = new IntWritable();
String str;
public void reduce(Text key, Iterable<IntWritable> values, Context context)
throws IOException, InterruptedException {
int sum = 0;
int cnt=0;
int avg;
for (IntWritable val : values) {
sum += val.get();
cnt++;
}
avg=sum/cnt;
gen.set(key);
context.write(new Text(gen),new IntWritable(avg));
if(avg>max){
max=avg;
gender.set(key);
count.set(max);
}
}
public void cleanup(Context context)throws IOException, InterruptedException
{
context.write(new Text("Maximum avg: "+gender),count);
}
}
public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
Job job = new Job(conf, "titanic");
job.setJarByClass(titanic.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(IntWritable.class);
job.setMapperClass(Map.class);
job.setReducerClass(Reduce.class);
job.setInputFormatClass(TextInputFormat.class); job.setOutputFormatClass(TextOutputFormat.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
job.waitForCompletion(true);
}
public IntWritable IntWritable(int avg) {
return null;
}
}
