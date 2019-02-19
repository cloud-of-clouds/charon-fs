package charon.storageService.repositories;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import charon.general.CharonConstants;

public class HopsFSConnectionFactory {
	
	public static boolean mountHDFSsuccess = false;
	
	public static DistributedFileSystem build() throws IOException {
		
		File hopsFS_CONF = new File(CharonConstants.HDFS_CONF_LOCATION_CONFIG_FILE_NAME);
		String hopsFSConfPath = null;
		try {
			InputStream in = new FileInputStream(hopsFS_CONF);
			Properties props = new Properties();
			props.load(in);
			hopsFSConfPath = props.getProperty("hopsFS_path_conf");
		} catch (FileNotFoundException e1) {
			throw new IOException("The config(hopsfsRep.config file is misconfigured");
		} catch (IOException e) {
			throw new IOException("The config(hopsfsRep.config file is misconfigured");
		}
		
		File hadoopConfFile = new File(hopsFSConfPath, "core-site.xml");
		if (!hadoopConfFile.exists()) {
			throw new IOException("No hadoop conf file: core-site.xml");
		}

		File yarnConfFile = new File(hopsFSConfPath, "yarn-site.xml");
		if (!yarnConfFile.exists()) {
			throw new IOException("No yarn conf file: yarn-site.xml");
		}

		File hdfsConfFile = new File(hopsFSConfPath, "hdfs-site.xml");
		if (!hdfsConfFile.exists()) {
			throw new IOException("No hdfs conf file: hdfs-site.xml");
		}
		Path yarnPath = new Path(yarnConfFile.getAbsolutePath());
		Path hdfsPath = new Path(hdfsConfFile.getAbsolutePath());
		Path hadoopPath = new Path(hadoopConfFile.getAbsolutePath());
		Configuration conf = new Configuration();
		conf.addResource(hadoopPath);
		conf.addResource(yarnPath);
		conf.addResource(hdfsPath);
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		mountHDFSsuccess = true;
		return (DistributedFileSystem) fs;
	}
	
}
