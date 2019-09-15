package hadoop_0811;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DataTransfer {
	
	Configuration localConf;
	Configuration hadoopConf;
	
	public DataTransfer(Configuration local, Configuration hadoop) {
		this.localConf = local;
		this.hadoopConf = hadoop;
	}
	
	public void localToHadoop() {
		try {
			// local file list
			Path localPath = new Path("D:\\in");
			FileSystem localSystem = FileSystem.getLocal(localConf);
			FileStatus[] localFileList = localSystem.listStatus(localPath);
			
			// hadoop file list
			Path hadoopPath = new Path("/in");
			FileSystem hadoopSystem = FileSystem.get(hadoopConf);
			FileStatus[] hadoopFileList = hadoopSystem.listStatus(hadoopPath);
			
			for (int i = 0; i < localFileList.length; i++) {
				String fileName = localFileList[i].getPath().getName();
				
				boolean check = true;
				for (int j = 0; j < hadoopFileList.length; j++) {
					if (fileName.equals(hadoopFileList[j].getPath().getName())) {
						check = false;
					}
				}
				
				if (check) {
					String inputPath = localPath + "/" + fileName;
					String outputPath = hadoopPath + "/" + fileName;
					
					FSDataInputStream fsdis = localSystem.open(new Path(inputPath));
					FSDataOutputStream fsdos = hadoopSystem.create(new Path(outputPath));
					
					int byteRead = 0;
					while ((byteRead = fsdis.read()) > 0) {
						fsdos.write(byteRead);
					}
					fsdis.close();
					fsdos.close();
					
					System.out.println("Moving data(" + fileName + ") from local to hadoop ... [" + (i + 1) + "/" + localFileList.length + "]");
					
				} else {
					System.out.println(fileName + "file already exists in hadoop :D");
					
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			
		}
	}

	public void hadoopToLocal(String path) {
		
		try {
			// local file list
			Path localPath = new Path(path);
			FileSystem localSystem = FileSystem.getLocal(localConf);
			FileStatus[] localFileList = localSystem.listStatus(localPath);
			
			// hadoop file list
			Path hadoopPath = new Path("/out");
			FileSystem hadoopSystem = FileSystem.get(hadoopConf);
			FileStatus[] hadoopFileList = hadoopSystem.listStatus(hadoopPath);
			
			for (int i = 1; i < hadoopFileList.length; i++) {
				String fileName = hadoopFileList[i].getPath().getName();
				
				boolean check = true;
				for (int j = 0; j < localFileList.length; j++) {
					if (fileName.equals(localFileList[j].getPath().getName())) {
						check = false;
					}
				}
				
				if (check) {
					String inputPath = hadoopPath + "/" + fileName;
					String outputPath = localPath + "/" + fileName;
					
					FSDataInputStream fsdis = hadoopSystem.open(new Path(inputPath));
					FSDataOutputStream fsdos = localSystem.create(new Path(outputPath));
					
					int byteRead = 0;
					while ((byteRead = fsdis.read()) > 0) {
						fsdos.write(byteRead);
					}
					fsdis.close();
					fsdos.close();
					
					System.out.println("Moving data(" + fileName + ") from hadoop to local ... [" + i + "/" + (hadoopFileList.length - 1) + "]");
					
				} else {
					System.out.println(fileName + "file already exists in hadoop :D");
					
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			
		}
	}
	
}
