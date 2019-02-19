package charon.storageService.repositories;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public class LocalFilesystemRepository implements FileRepository {

	private String path;

	public LocalFilesystemRepository(String path) {
		//Local Repository should not end with '/'
		if(path.endsWith(File.separator))
			path = path.substring(0, path.length()-1);

		this.path = path;
		File f = new File(path);
		while(!f.exists())
			f.mkdirs();
	}

	@Override
	public byte[] read(String filename) {
		synchronized (filename.intern()) {
			File file = new File(path + File.separator + filename);
			if(!file.exists())
				return null;

			RandomAccessFile rand = null;
			try {
				rand = new RandomAccessFile(file, "r");
				byte[] res = new byte[(int)file.length()];
				rand.read(res);
				rand.close();
				return res;
			} catch (IOException e) {}

			return null;
		}
	}

	//	@Override
	//	public String getHash(String filename) {
	//		File file = new File(path + File.separator + filename);
	//		return getHash(file);
	//	}

	@Override
	public long write(String filename, byte[] data) {
		return write(filename, data, 0);
	}

	@Override
	public long write(String filename, byte[] data, long offset) {
		synchronized (filename.intern()) {
			File file = new File(path + File.separator + filename);
			RandomAccessFile rand = null;
			while(!file.exists()){
				if(filename.contains(File.separator)){
					File f=new File(path + File.separator + filename.substring(0,filename.lastIndexOf(File.separator)));
					while(!f.exists())
						f.mkdirs();
				}
				try {
					file.createNewFile();
				} catch (IOException e1) {}
			}


			try {
				rand = new RandomAccessFile(file, "rw");
			} catch (FileNotFoundException e) { /* THIS WILL NEVER HAPPEN */ }
			//			}

			try {
				rand.seek(offset);
				rand.write(data);
				rand.close();
			} catch (IOException e) {
				e.printStackTrace();
				return -1;
			}

			return file.length();
		}
	}

	@Override
	public byte[] read(String filename, long offset, int blocksize, String externalManaged) {
		synchronized (filename.intern()) {
			File file = new File(externalManaged);
			if(!file.exists())
				return null;

			RandomAccessFile rand = null;
			try {
				rand = new RandomAccessFile(file, "r");
				byte[] res = null;
				if((offset+blocksize)<file.length())
					res = new byte[blocksize];
				else{
					if(file.length()-offset >= 0)
						res= new byte[(int)(file.length()-offset)];
					else{
						rand.close();
						return new byte[0];
					}
				}
				rand.seek(offset);
				rand.read(res);
				rand.close();
				return res;
			} catch (IOException e) {e.printStackTrace();}

			return null;
		}
	}

	@Override
	public byte[] read(String filename, long offset, int blocksize) {
		synchronized (filename.intern()) {
			File file = new File(path + File.separator + filename);
			if(!file.exists())
				return null;

			RandomAccessFile rand = null;
			try {
				rand = new RandomAccessFile(file, "r");
				byte[] res = null;
				if((offset+blocksize)<file.length())
					res = new byte[blocksize];
				else{
					if(file.length()-offset >= 0)
						res= new byte[(int)(file.length()-offset)];
					else{
						rand.close();
						return new byte[0];
					}
				}
				rand.seek(offset);
				rand.read(res);
				rand.close();
				return res;
			} catch (IOException e) {e.printStackTrace();}

			return null;
		}
	}

	@Override
	public void truncate(String filename, long totalSize) {
		synchronized (filename.intern()) {
			File file = new File(this.path + File.separator + filename);
			while(!file.exists()){

				File f=new File(path + File.separator + filename.substring(0,filename.lastIndexOf(File.separator)));
				while(!f.exists())
					f.mkdirs();

				try {
					file.createNewFile();
				} catch (IOException e1) {}
			}

			try {
				FileOutputStream fout = new FileOutputStream(file);
				FileChannel outChan = fout.getChannel();
				outChan.truncate(totalSize);
				outChan.close();
				fout.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}


	//	private String getHash(File f){
	//		if(!f.exists())
	//			return null;
	//		return IntegrityManager.getHexHash((""+f.lastModified()).getBytes());
	//	}


}
