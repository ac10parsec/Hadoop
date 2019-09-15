package com.hadoop_0808;

import org.apache.hadoop.conf.Configuration;

public class Main {
	
	public static void main(String[] args) {
		if (args.length != 4) {
			System.out.println("The number of path is not 4");
			System.exit(1);
		}
		
		String localInPath = args[0];
		String localOutPath = args[1];
		String hadoopInPath = args[2];
		String hadoopOutPath = args[3];
		
		Configuration conf = new Configuration();
		DataTransfer dt = new DataTransfer();
		DataAnalysis da = new DataAnalysis();
		
		// data upload from local to hadoop
		dt.inputData(conf, localInPath, hadoopInPath);
		System.out.println("Data transmission done.");
		
		// data analysis >> MapReduce
		da.departureDelayCount(conf, hadoopInPath, hadoopOutPath);
		System.out.println("Data analysis done.");
		
		// data download from hadoop to local
		dt.outputData(conf, hadoopOutPath, localOutPath);
	}

}
