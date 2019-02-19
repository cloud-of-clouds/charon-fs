package charon.storageService;
import java.nio.ByteBuffer;
import java.util.Map;

import charon.configuration.Location;
import depsky.client.messages.metadata.ExternalMetadata;


public interface IStorageService{

	public long writeData(String fileId, ByteBuffer buf, int offset, boolean isPending, long totalSize, Map<Integer, ExternalMetadata> hashs, Location location);
	
	public int readData(String fileId, ByteBuffer buf, int offset, int capacity, Map<Integer,ExternalMetadata> hashs, boolean isPending, long totalSize, Location location, String externalManaged);
	
	public int truncData(String path, String fileId, int size, Map<Integer, ExternalMetadata> hashs, boolean isToSyncWClouds, boolean isPending, long totalSize, Location location);
	
	public int deleteData(String fileId, Location location);
	
	public void syncWDisk(String fileId, Location location);
	
	public int syncWClouds(String fileId, Location location);
	
	public int updateCache(String fileId, ExternalMetadata hash, boolean isPending, int block, long totalSize, Location location, String externalManaged);

	public int releaseData(String fileId, boolean isPending);

}
