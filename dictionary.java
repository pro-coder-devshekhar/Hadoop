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
public class dictionary
{
public static class Map extends Mapper<LongWritable, Text, Text, Text>
{
public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
{
String[] line = value.toString().split(",");
for(String lines:line){
try{
context.write(new Text(line[0]), new Text(lines));
}
catch(Exception e){
context.write(new Text(line[0]), new Text() );
}
}
}
}
public static class depart extends Partitioner<Text,Text>{
public int getPartition(Text key,Text value,int nr){
String a=key.toString();
if(a.toUpperCase().equals("BAD"))
return 0;
else
return 1;
}
}
public static class Reduce extends Reducer<Text, Text, Text, Text> {
int b=0;String a,c;
Text wrd1=new Text();
IntWritable res1=new IntWritable();
public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
int sum = 0;
context.write(key,new Text("has following synonyms:"));
for (Text val : values) {
c=val.toString();
if(c.toUpperCase().equals("BIG"))
break;
else{
context.write(new Text(),val);
sum ++;
}
}
if(sum>b){
b=sum;
wrd1.set(key);
res1.set(b);
a=String.valueOf(b);
}
}
public void cleanup(Context context)throws IOException, InterruptedException{
context.write(new Text("Maximum synonyms for "+wrd1+" is:"),new Text(a));
}
}
public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
Job job = new Job(conf, "dictionary");
job.setJarByClass(dictionary.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);
job.setMapperClass(Map.class);
job.setReducerClass(Reduce.class);
job.setInputFormatClass(TextInputFormat.class);
job.setOutputFormatClass(TextOutputFormat.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
job.setPartitionerClass(depart.class);
job.setNumReduceTasks(2);
job.waitForCompletion(true);
}
}