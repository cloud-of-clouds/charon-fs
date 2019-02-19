package charon.storageService.repositories;

import java.io.IOException;

public class FileRepositoryFactory {

	public static FileRepository build(String path, String cache){
		if(path.startsWith("hopsfs:")){
			path = path.substring("hopsfs:".length());
			try {
				return new HopsFSRepository(HopsFSConnectionFactory.build(), path, cache);
			} catch (IOException e) {
				System.out.println("\nWARNING: No HDFS configuration dir founded!");
				System.out.println("WARNING: The system will mount without a HOPS-FS repository!\n");
			}
			return null;
			//return new HopsFSRepository("path/to/hdfs", "folderNameInHDFS",cache);
		}
		return new LocalFilesystemRepository(path);
	}

}
