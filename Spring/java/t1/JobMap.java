package t1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JobMap extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String[] values = value.toString().split(",");
		String strKey = values[0];
		Text textKey = new Text();
		textKey.set(strKey); 
		IntWritable intValue = new IntWritable(1);
		context.write(textKey, intValue);
	}
	
	

}
