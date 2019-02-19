package charon.directoryService.externalManagement;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import depsky.util.integrity.IntegrityManager;

public class HopsFSExternalFile implements ExternalFile{
	
	private DistributedFileSystem fs;
	private Path path;

	public HopsFSExternalFile(DistributedFileSystem fs , String fullPath){
		this.fs = fs;
		this.path = new Path(fullPath);
	}

	@Override
	public boolean exists() {
		try {
			return fs.exists(path);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public boolean isDirectory() {
		try {
			return fs.getFileStatus(path).isDirectory();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public String getName() {
		return path.getName();
	}

	@Override
	public boolean isFile() {
		try {
			return fs.getFileStatus(path).isFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public ExternalFile[] listFiles() {
		try {
			if(isDirectory()){
				FileStatus[] list = fs.listStatus(path);
				ExternalFile[] res = new ExternalFile[list.length];
				
				for(int i = 0; i < list.length; i++){
					res[i] = (ExternalFile) new HopsFSExternalFile(fs, list[i].getPath().toString());
				}
				return res;
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public long lastModifiedTime() {
		try {
			return fs.getFileStatus(path).getModificationTime();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return -1;
	}

	@Override
	public long lastAccessTime() {
		try {
			return fs.getFileStatus(path).getAccessTime();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return -1;
	}

	@Override
	public long creationTime() {
		return 0;
	}

	@Override
	public long size() {
		try {
			return fs.getFileStatus(path).getLen();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return -1;
	}

	@Override
	public String getPathIdentifier() {
		return Path.getPathWithoutSchemeAndAuthority(path).toString();
	}

	@Override
	public void read(ByteBuffer buf, int offset) {
		try {
			FSDataInputStream input = fs.open(path);
			input.seek(offset);
			input.read(buf);
			input.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public String getHash() {
		try {
			return IntegrityManager.getHexHash(fs.getFileChecksum(path).getBytes());
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}	
}
