package com.hadoop_0808;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DataTransfer {
	
	public void inputData(Configuration conf, String localInPath, String hadoopInPath) {
		Path localPath = new Path(localInPath);
		Path hadoopPath = new Path(hadoopInPath);
		
		try {
			FileSystem localSystem = FileSystem.getLocal(conf);
			FileSystem hadoopSystem = FileSystem.get(conf);
			
			FileStatus[] localFileList = localSystem.listStatus(localPath);
			FileStatus[] hadoopFileList = hadoopSystem.listStatus(hadoopPath);
			
			for (int i = 0; i < localFileList.length; i++) {
				String fileName = localFileList[i].getPath().getName();
				
				boolean check = true;			
				for (int j = 0; j < hadoopFileList.length; j++) {
					if ((hadoopFileList[j].getPath().getName()).equals(fileName)) {
						check = false;
					}
				}
				
				if (check) {
					//System.out.println(fileList[i].getPath().getName());
					String inputPath = localInPath + "/" + fileName;
					String outputPath = hadoopInPath + "/" + fileName;
					System.out.println(inputPath + " >>> " + outputPath);
					
					FSDataInputStream fsdis = localSystem.open(new Path(inputPath));
					FSDataOutputStream fsdos = hadoopSystem.create(new Path(outputPath));
					
					int byteRead = 0;
					while ((byteRead = fsdis.read()) > 0) {
						fsdos.write(byteRead);
					}
					fsdis.close();
					fsdos.close();
					
					System.out.println("Moving data from local to hadoop ... (done: " + (i + 1) + "/" + localFileList.length + ")");
					
				} else {
					System.out.println(fileName + " file already exists in hadoop :D");
					
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void outputData(Configuration conf, String hadoopOutPath, String localOutPath) {
		Path localPath = new Path(localOutPath);
		Path hadoopPath = new Path(hadoopOutPath);
		
		try {
			FileSystem localSystem = FileSystem.getLocal(conf);
			FileSystem hadoopSystem = FileSystem.get(conf);
			
			FileStatus[] localFileList = localSystem.listStatus(localPath);
			FileStatus[] hadoopFileList = hadoopSystem.listStatus(hadoopPath);
			
			
			for (int i = 0; i < hadoopFileList.length; i++) {
				String fileName = hadoopFileList[i].getPath().getName();
				System.out.println(fileName);
				
				boolean check = true;
				for (int j = 0; j < localFileList.length; j++) {
					if ((localFileList[j].getPath().getName()).equals(fileName)) {
						check = false;
					}
				}
				
				if (check) {
					//System.out.println(fileList[i].getPath().getName());
					String inputPath = hadoopOutPath + "/" + fileName;
					String outputPath = localOutPath + "/" + fileName;
					System.out.println(inputPath + " >>> " + outputPath);
					
					FSDataInputStream fsdis = hadoopSystem.open(new Path(inputPath));
					FSDataOutputStream fsdos = localSystem.create(new Path(outputPath));
					
					int byteRead = 0;
					while ((byteRead = fsdis.read()) > 0) {
						fsdos.write(byteRead);
					}
					fsdis.close();
					fsdos.close();
					
					System.out.println("Moving data from hadoop to local ... (done: " + (i + 1) + "/" + localFileList.length + ")");
					
				} else {
					System.out.println(fileName + " file already exists in local :D");
					
				}
				
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
}
