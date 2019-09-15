package hadoop_0812;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DepartureDelayCountReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {

	//private IntWritable result = new IntWritable();
	private FloatWritable result = new FloatWritable();

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
		
		float sum = 0;
		for (IntWritable value : values) {
			sum += (float) value.get();
		}
		
		if (sum >= 60) {
			sum /= 60.0;
		}
		
		result.set(sum);
		context.write(key, result);
	}
	
}