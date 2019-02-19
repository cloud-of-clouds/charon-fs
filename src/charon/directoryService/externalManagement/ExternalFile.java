package charon.directoryService.externalManagement;

import java.io.Serializable;
import java.nio.ByteBuffer;


public interface ExternalFile extends Serializable{

	public boolean exists();

	public boolean isDirectory();

	public String getName();

	public boolean isFile();

	public ExternalFile[] listFiles();

	public long lastModifiedTime();

	public long lastAccessTime();

	public long creationTime();

	public long size();

	public String getPathIdentifier();
	
	public void read(ByteBuffer buf, int offset);
	
	public String getHash();
	
}
