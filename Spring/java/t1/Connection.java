package t1;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobMapTaskRescheduledEvent;

public class Connection extends HttpServlet {
	
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		System.out.println("doGet()");
		
		// local information
		Configuration localConf = new Configuration();
		
		FileSystem localSystem = FileSystem.getLocal(localConf);		
		FileStatus[] localFileList = localSystem.listStatus(new Path("D:\\IDE\\workspace\\hdfs"));
		for (int i = 0; i < localFileList.length; i++) {
			//System.out.println(localFileList[i].getPath().getName());
		}
		
		// hadoop information
		Configuration hadoopConf = new Configuration();
		hadoopConf.set("fs.defaultFS", "hdfs://silverstar:9000");
		
		/*
		// MapReduce
		try {
			Job job = Job.getInstance(hadoopConf, "test");
			job.setJarByClass(Connection.class);
			job.setMapperClass(JobMap.class);
			job.setReducerClass(JobReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			job.setNumReduceTasks(1);
			FileInputFormat.addInputPath(job, new Path("/in"));
		    FileOutputFormat.setOutputPath(job, new Path("/output"));
			job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
		*/
		FileSystem hadoopSystem = FileSystem.get(hadoopConf);	
		FileStatus[] hadoopFileList = hadoopSystem.listStatus(new Path("/output"));
		for (int i = 0; i < hadoopFileList.length; i++) {
			System.out.println(hadoopFileList[i].getPath().getName());
		}
		
		PrintWriter pw = response.getWriter();
		if(hadoopFileList.length > 1) {
			String targetPath = "/output/" + hadoopFileList[1].getPath().getName();
			FSDataInputStream fsis = hadoopSystem.open(new Path(targetPath));
			int byteRead = 0;
			while((byteRead = fsis.read()) > 0) { 
				pw.write(byteRead);
			}
			return;
		}
		
		response.getWriter().write("Hello Hadoop :D");
	}

}
