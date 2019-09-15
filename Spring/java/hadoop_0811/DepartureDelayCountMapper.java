package hadoop_0811;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DepartureDelayCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	private Text outputKey = new Text(); // output key
	private IntWritable outputValue = new IntWritable(); // output value
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
	
		Parser parser = new Parser(value);
		System.out.println(parser.getUniqueCarrier());
		outputKey.set(parser.getYear() + ", " + parser.getMonth() + ", " + parser.getUniqueCarrier() + ": ");
		outputValue.set(parser.getActualElapsedTime());
		
		if (parser.getDepartureDelayTime() > 0) {
			context.write(outputKey, outputValue);
		}
	}
	
}
