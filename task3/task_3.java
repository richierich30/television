package task3;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class task_3{
	
private enum COUNTER{
	INVALID_RECORD_COUNT
}
public static final int COMPANY_NAME=0;
public static final int PRODUCT_NAME =1;
public static final int SIZE=2;
public static final int STATE=3;
public static final int PIN_CODE=4;
public static final int PRICE=5;

public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> 
{
	public final static IntWritable one = new IntWritable(1);
		
	public void map(LongWritable key, Text value,Context context) throws IOException , InterruptedException
	{
		String[] a = value.toString().split("[|]");
		Text CompanyName = new Text(a[0]);
		Text state = new Text(a[3]);
		Text prod = new Text(a[1]);
		if(!CompanyName.equals(new Text("NA"))){
			if(!prod.equals(new Text("NA"))){
				if(CompanyName.equals(new Text("Onida")))
				{
					context.write(state, one);
				}
			}
		}
		}
	}
   public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (args.length != 2)
		{
			System.err.println("Usage: invalid_record <in> <out>");
			System.exit(2);
		}
		@SuppressWarnings("deprecation")
		Job job = new Job(conf,"Number of units sold");
		job.setJarByClass(task_3.class);
		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);
		
		org.apache.hadoop.mapreduce.Counters counter= job.getCounters();
		System.out.println("Number of Invalid Records: "+ counter.findCounter(COUNTER.INVALID_RECORD_COUNT).getValue());

	}

	}



