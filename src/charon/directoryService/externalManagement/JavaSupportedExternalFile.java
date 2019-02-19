package charon.directoryService.externalManagement;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;

public class JavaSupportedExternalFile implements ExternalFile {

	private File file;

	public JavaSupportedExternalFile(String path) {
		this.file = new File(path);
	}

	public JavaSupportedExternalFile(File file) {
		this.file = file;
	}

	@Override
	public boolean exists() {
		return file.exists();
	}

	@Override
	public boolean isDirectory() {
		return file.isDirectory();
	}

	@Override
	public String getName() {
		return file.getName();
	}

	@Override
	public boolean isFile() {
		return file.isFile();
	}

	@Override
	public ExternalFile[] listFiles() {
		return filesToExternalFiles(file.listFiles());
	}

	@Override
	public long lastModifiedTime() {
		try {
			BasicFileAttributes attr = Files.readAttributes(file.toPath(), BasicFileAttributes.class);
			return attr.lastModifiedTime().toMillis();
		} catch (IOException e) {
			e.printStackTrace();
			return 0;
		}
	}

	@Override
	public long lastAccessTime() {
		try {
			BasicFileAttributes attr = Files.readAttributes(file.toPath(), BasicFileAttributes.class);
			return attr.lastAccessTime().toMillis();
		} catch (IOException e) {
			e.printStackTrace();
			return 0;
		}
	}

	@Override
	public long creationTime() {
		try {
			BasicFileAttributes attr = Files.readAttributes(file.toPath(), BasicFileAttributes.class);
			return attr.creationTime().toMillis();
		} catch (IOException e) {
			e.printStackTrace();
			return 0;
		}
	}

	@Override
	public long size() {
		try {
			BasicFileAttributes attr = Files.readAttributes(file.toPath(), BasicFileAttributes.class);
			return attr.size();
		} catch (IOException e) {
			e.printStackTrace();
			return 0;
		}
	}

	private ExternalFile[] filesToExternalFiles(File[] files){
		if(files==null) return null;

		ExternalFile[] res = new ExternalFile[files.length];

		for(int i = 0 ; i < files.length ; i++){
			res[i] = new JavaSupportedExternalFile(files[i]);
		}

		return res;
	}

	@Override
	public String getPathIdentifier() {
		return file.getAbsolutePath();
	}

	@Override
	public void read(ByteBuffer buf, int offset) {
		try {
			RandomAccessFile rand = new RandomAccessFile(file, "r");
			FileChannel channel = rand.getChannel();
			channel.position(offset);
			int cont = 1;
			while(cont>0)
				cont = channel.read(buf);
			
			channel.close();
			rand.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return;
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
	}

	@Override
	public String getHash() {
		return String.valueOf(lastModifiedTime());
	}
	
}
