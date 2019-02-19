package charon.storageService.repositories;

public interface FileRepository {
	
	public long write(String filename, byte[] data);
	
	public long write(String filename, byte[] data, long offset);
	
	public byte[] read(String filename, long offset, int blocksize);

	public byte[] read(String filename);

	public void truncate(String path, long totalSize);
	

	public byte[] read(String filename, long offset, int blocksize, String externalManaged);
	
}
