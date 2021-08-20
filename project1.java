package Dev_hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.json.simple.parser.*;


public class project1
{

    public static class Map extends Mapper<LongWritable, Text, Text, Text>
    {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
        	String jsondata = value.toString();
        	jsondata = jsondata.replace("{", "");
        	jsondata = jsondata.replace("}", "");
        	jsondata = jsondata.replace("\"", "");
        	String jparts[] = jsondata.split(",");
        	String finalResult = "";
        	
        	for(String jvalue : jparts)
        	{
        		String sigleval[] = jvalue.split(":");
        		finalResult = finalResult + "," + sigleval[1].toString();
        	}
        	
        	context.write(new Text(""), new Text(finalResult));
        }
    }

    public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>
    {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
        	int count=0;
        	for(IntWritable outval : values)
        		count = count + 1;
        	
        	context.write(new Text(key), new IntWritable(count));
        }
    }

    public static void main(String[] args) throws Exception 
    {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "project1");
        job.setJarByClass(project1.class);
        job.setMapperClass(Map.class);
      //job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}