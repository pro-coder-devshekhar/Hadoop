package Dev_hadoop;
import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
public class student1
{
public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
{
Path[ ] cfile=new Path[0];
ArrayList<Text> dep=new ArrayList<Text>();
public void setup(Context context)
{
Configuration conf=context.getConfiguration();
try
{
cfile = DistributedCache.getLocalCacheFiles(conf);
BufferedReader reader=new BufferedReader(new FileReader(cfile[0].toString()));
String line;
while ((line=reader.readLine())!=null)
{
Text tt=new Text(line);
dep.add(tt);
}
}
catch(IOException e)
{
e.printStackTrace();
}
}
public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
{
String line2 = value.toString();
String[ ] elements=line2.split(",");
int val=0;
for(int i=2;i<7;i++)
{
int j=Integer.parseInt(elements[i]);
val=val+j;
}
for(Text e:dep)
{
String[] line1 = e.toString().split(",");
if(Integer.parseInt(elements[0])==Integer.parseInt(line1[0]))
{
context.write(new Text(elements[0]+" "+elements[1]+" "+line1[1]),new IntWritable(val));
}
}
}
}
public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
{
int k=0;int avg=0,b=0;
Text wrd1=new Text();
int sum = 0;
IntWritable res1=new IntWritable();
IntWritable res2=new IntWritable();
public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
{
for(IntWritable a:values)
{
sum = a.get();
res2.set(sum);
avg=sum/5;
wrd1.set(key);
res1.set(sum);
res2.set(avg);
context.write(new Text(wrd1+"'s total mark is:"),res1);
context.write(new Text(wrd1+"'s average mark is:"),res2);
}
}
public void cleanup(Context context)throws IOException, InterruptedException
{}
}
public static void main(String[] args) throws Exception
{
Configuration conf = new Configuration();
Job job = new Job(conf, "dcache");
job.setJarByClass(student1.class);
job.setOutputKeyClass(Text.class);
DistributedCache.addCacheFile(new Path(args[0]).toUri(),job.getConfiguration());
job.setOutputValueClass(IntWritable.class);
job.setMapperClass(Map.class);
job.setReducerClass(Reduce.class);
job.setInputFormatClass(TextInputFormat.class);
job.setOutputFormatClass(TextOutputFormat.class);
FileInputFormat.addInputPath(job, new Path(args[1]));
FileOutputFormat.setOutputPath(job, new Path(args[2]));
job.waitForCompletion(true);
}
}