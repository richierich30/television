package television;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class invalid_record {
	
private enum COUNTER{
	INVALID_RECORD_COUNT
}
public static final int COMPANY_NAME=0;
public static final int PRODUCT_NAME =1;
public static final int SIZE=2;
public static final int STATE=3;
public static final int PIN_CODE=4;
public static final int PRICE=5;

public static class Map extends Mapper<LongWritable, Text, Text, Text> 
{
	Text CompanyName = new Text();
	Text ProductName = new Text();
	
	public void map(LongWritable key, Text value,Context context) throws IOException , InterruptedException
	{
		String[] a = value.toString().split("[|]");
		String z=a[invalid_record.COMPANY_NAME].toString();
		String y=a[invalid_record.PRODUCT_NAME].toString();
		
		if(z.equals("NA")|| y.equals("NA"))
		{
			context.getCounter(COUNTER.INVALID_RECORD_COUNT).increment(1L);
		}
		else
		{
			CompanyName.set(z);
			ProductName.set(y);
		context.write(CompanyName, ProductName);	
	}
}
}
public static class Reduce extends Reducer<Text, Text, Text, Text>
{
	
	public void reduce(Text companyname, Text prodname,Context context)throws IOException,InterruptedException
	{
		Text CompanyName = companyname;
		Text ProductName = prodname;
		
		context.write(CompanyName, ProductName);
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
		Job job = new Job(conf,"Number of invalid records");
		job.setJarByClass(invalid_record.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);
		
		org.apache.hadoop.mapreduce.Counters counter= job.getCounters();
		System.out.println("Number of Invalid Records: "+ counter.findCounter(COUNTER.INVALID_RECORD_COUNT).getValue());

	}
}


