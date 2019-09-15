package com.hadoop_0807;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DepartureDelayCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	private Text outputKey = new Text(); // output key
	private final static IntWritable outputValue = new IntWritable(1); // output value
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
	
		Parser parser = new Parser(value);
		
		outputKey.set(parser.getYear() + "," + parser.getMonth());
		
		if (parser.getDepartureDelayTime() > 0) {
			context.write(outputKey, outputValue);
		}
	}
	
}
