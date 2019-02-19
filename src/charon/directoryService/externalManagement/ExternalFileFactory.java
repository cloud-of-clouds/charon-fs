package charon.directoryService.externalManagement;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import charon.storageService.repositories.HopsFSConnectionFactory;

public class ExternalFileFactory {

	private static DistributedFileSystem fs;

	public static ExternalFile build(String pathId){
		if(pathId.startsWith("hopsfs:")){
			//String auxPathid = pathId.substring("hopsfs://".length());
			//String host = auxPathid.substring(0, auxPathid.indexOf("/"));
			String path = pathId.substring("hopsfs:".length());
			if(fs==null){
				try {
//					Configuration conf = new Configuration();
//					URI uri = new URI("hdfs://" + host);
//					fs = FileSystem.get(uri, conf);
					fs = HopsFSConnectionFactory.build();
				} catch (IOException e) {
					//System.out.println("ERROR: No HDFS configuration dir founded!");
					return null;
					//System.out.println("ERROR: the system will mount with out a HOPS-FS repository ");
				}
			}
			return new HopsFSExternalFile(fs, path);
		}else{
			return new JavaSupportedExternalFile(pathId);
		}
	}

}
