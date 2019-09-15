package hadoop_0812;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Main extends HttpServlet {

	protected void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
		
	}
	
	protected void doPost(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
		// local information
		Configuration localConf = new Configuration();
		
		// hadoop information
		Configuration hadoopConf = new Configuration();
		hadoopConf.set("fs.defaultFS", "hdfs://192.168.3.179:9000");
		
		DataTransfer dt = new DataTransfer(localConf, hadoopConf);
		
		// Data transmission from lcoal to hadoop
		dt.localToHadoop();
		
		// Set input and output path in hadoop
		Path inputPath = new Path("/in");
		Path outputPath = new Path("/out");
		
		// Check existence of output path of mapreduce
		FileSystem hadoopSystem = FileSystem.get(hadoopConf);
		
		if (hadoopSystem.exists(outputPath)) {
			hadoopSystem.delete(outputPath);
		}
		
		System.out.println("Start MapReduce");
		// MapReduce
		try {
			Job job = Job.getInstance(hadoopConf, "DepartureDelayCount");
			
			job.setJarByClass(Main.class);
			job.setMapperClass(DepartureDelayCountMapper.class);
			job.setReducerClass(DepartureDelayCountReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			job.setNumReduceTasks(1);
			FileInputFormat.addInputPath(job, inputPath);
		    FileOutputFormat.setOutputPath(job, outputPath);  
			job.waitForCompletion(true);
			
		} catch (Exception e) {
			e.printStackTrace();
			
		}
		System.out.println("End MapReduce");
		
		// Print
		FileStatus[] hadoopFileList = hadoopSystem.listStatus(new Path("/out"));
		PrintWriter pw = res.getWriter();
		for (int i = 1; i < hadoopFileList.length; i++) {
			String targetPath = "/out/" + hadoopFileList[i].getPath().getName();
			FSDataInputStream fsdis = hadoopSystem.open(new Path(targetPath));
			/*
			int byteRead = 0;
			while((byteRead = fsdis.read()) > 0) { 
				pw.write(byteRead);
			}
			*/
			BufferedReader br = new BufferedReader(new InputStreamReader(fsdis));
			StringBuffer response = new StringBuffer();
			String line = "";
			while ((line = br.readLine()) != null) {
				pw.write(line + " (hour)");
				pw.write("\r");
			}
		}
		//res.getWriter().write("Hello Hadoop :D");
		
		// Data transmission from hadoop to local
		String savePath = req.getParameter("savePath");
		if (!savePath.equals("")) {
			dt.hadoopToLocal(savePath);
			System.out.println("Saving a result in " + savePath);
		}
	}

}
