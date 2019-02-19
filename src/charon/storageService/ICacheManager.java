package charon.storageService;

import java.nio.ByteBuffer;

public interface ICacheManager {

	public int read(String fileId, ByteBuffer buf, int offset, int capacity );
	
	public int write(String fileId, ByteBuffer buf, int srcOffset,int dataLen);
	
	public byte[] readWhole(String fileId);
	
	public int writeWhole(String fileId, byte[] data);
	
	public int truncate(String fileId, int size);
	
	public int delete(String fileId);
	
	public boolean isInCache(String fileId);
}
