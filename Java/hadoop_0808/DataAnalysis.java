package com.hadoop_0808;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.hadoop_0807.DepartureDelayCountMapper;
import com.hadoop_0807.DepartureDelayCountReducer;

public class DataAnalysis {
	
	public void departureDelayCount(Configuration conf, String hadoopInPath, String hadoopOutPath) {
		
		try {
			Job job = Job.getInstance(conf, "DepartureDelayCount");
			
			job.setJarByClass(DataAnalysis.class);
			job.setMapperClass(DepartureDelayCountMapper.class);
			job.setReducerClass(DepartureDelayCountReducer.class);
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			FileInputFormat.addInputPath(job, new Path(hadoopInPath));
			FileOutputFormat.setOutputPath(job, new Path(hadoopOutPath));
			
			System.exit(job.waitForCompletion(true) ? 0 : 1);
			
		} catch (Exception e) {
			e.printStackTrace();
			
		}
		
	}
	
}
