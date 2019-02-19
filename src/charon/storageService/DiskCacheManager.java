package charon.storageService;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import charon.util.CharonPair;


public class DiskCacheManager implements ICacheManager{

	private String cacheDir;
	private String cache;
	private Map<String, CharonPair<RandomAccessFile, FileChannel>> channels;


	public DiskCacheManager(String cache){
		this.cache = (cache.endsWith(File.separator) ? cache : cache + File.separator);
		this.cacheDir = this.cache + "data directory" + File.separator;
		File f = new File(cacheDir);
		if(!f.exists() )
			while(!f.mkdirs());

		channels = new ConcurrentHashMap<String, CharonPair<RandomAccessFile, FileChannel>>();
	}


	@Override
	public int read(String fileId, ByteBuffer buf, int offset, int capacity) {
		try {
			File file = new File(cacheDir+fileId);
			CharonPair<RandomAccessFile, FileChannel> pair = getFileAccessors(fileId, file, "r", false);
			if(pair==null)
				return -1;

			//			rand.seek(offset);

			//			FileChannel channel = pair.getV();
			//			if(channel.position() != offset){
			//				//				System.out.println("R: nao estava no mesmo. - " + fileId + " - (offset:  " + offset +", channelOff: " + channel.position());
			//				channel.position(offset);
			//			}
			//
			//			int n = 1;
			//			while(n>0)
			//				n = channel.read(buf);

			RandomAccessFile rand = pair.getK();
			if(rand.getFilePointer() != offset){
				rand.seek(offset);
			}
			byte[] array = new byte[capacity];
			rand.read(array);


			buf.put(array);

			sync(fileId);

			//			channel.close();
			//			rand.close();
			return 0;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return -1;
	}

	@Override
	public byte[] readWhole(String fileId) {
		try {
			File file = new File(cacheDir+fileId);
			RandomAccessFile rand = null;
			try{
				rand = new RandomAccessFile(file, "r");
			}catch(FileNotFoundException e){
				file = getFileIntChache(file, false);
				if(file==null)
					return null;
			}

			byte[] result = new byte[((int) file.length())];
			rand.read(result);
			rand.close();

			return result;

		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	@Override
	public int writeWhole(String fileId, byte[] value) {
		try {
			File file = new File(cacheDir+fileId);
			RandomAccessFile rand = null;
			try{
				rand = new RandomAccessFile(file, "rw");
			}catch(FileNotFoundException e){
				file = getFileIntChache(file, true);
				if(file==null)
					return -1;
				rand = new RandomAccessFile(file, "rw");
			}

			if(value.length > file.length()){
				freeSomeMemory(value.length);
				rand.setLength(value.length);
			}
			rand.write(value);
			rand.close();
			return 0;
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
		return -1;
	}

	public int writeWhole(String fileId, byte[] value, int len ) {
		try {
			File file = new File(cacheDir+fileId);
			RandomAccessFile rand = null;
			try{
				rand = new RandomAccessFile(file, "rw");
			}catch(FileNotFoundException e){
				file = getFileIntChache(file, false);
				if(file==null)
					return -1;
			}

			if(len > file.length()){
				freeSomeMemory(len);
				rand.setLength(len);
			}
			rand.write(value, 0, len);
			rand.close();
			return 0;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return -1;
	}

	@Override
	public int write(String fileId, ByteBuffer buf, int dstOffset, int dataLen) {

		try {
			File file = new File(cacheDir+fileId);
			CharonPair<RandomAccessFile, FileChannel> pair = getFileAccessors(fileId, file, "rw", true);
			if(pair==null)
				return -1;

			//			if(dstOffset+dataLen > file.length()){
			//				freeSomeMemory(dstOffset+dataLen);
			//			}
			//			rand.seek(dstOffset);
			//			FileChannel channel = pair.getV();
			//
			//			if(channel.position()!=dstOffset){
			//				//				System.out.println("W: nao estava no mesmo. (destOff:  " + dstOffset +", channelOff: " + channel.position());
			//				channel.position(dstOffset);
			//			}
			//
			//
			//			int written = 0;
			//			while(written < dataLen ){
			//				written += channel.write(buf);
			//			}

			RandomAccessFile rand = pair.getK();
			if(rand.getFilePointer() != dstOffset)
				rand.seek(dstOffset);

			byte[] dest = new byte[dataLen];
			buf.get(dest);
			rand.write(dest);

			//			if(written > dataLen)
			//				throw new RuntimeException("Escrevi mais do que devia! - written = " + written +", dataLen = " + dataLen);

			//			channel.close();
			//			rand.close();
			sync(fileId);
			return 0;
		} catch (IOException e) {
			e.printStackTrace();
		}

		return -1;
	}

	public int truncate(String fileId, int size) {

		try {
			File file = new File(cacheDir+fileId);

			RandomAccessFile rand = null;
			try{
				rand = new RandomAccessFile(file, "rw");
			}catch(FileNotFoundException e){
				file = getFileIntChache(file, false);
				if(file==null)
					return -1;
			}

			rand.setLength(size);
			rand.close();

			return 0;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return -1;

	}

	public int delete(String fileId) {
		sync(fileId);
		File file = new File(cacheDir+fileId);

		while(file.exists())
			file.delete();

		return 0;
	}

	public boolean isInCache(String fileId) {
		return new File(cacheDir+fileId).exists();
	}

	public void sync(String fileId)  {
		CharonPair<RandomAccessFile, FileChannel> pair = channels.remove(fileId);
		if(pair==null){
			return;
		}

		try {
			freeSomeMemory(pair.getV().size());
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		try {
			pair.getV().close();
			pair.getK().close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private CharonPair<RandomAccessFile, FileChannel> getFileAccessors(String fileId, File file, String mode, boolean create){

		CharonPair<RandomAccessFile, FileChannel> pair = channels.get(fileId);
		if(pair != null){
			return pair;
		}

		RandomAccessFile rand;
		try{
			rand = new RandomAccessFile(file, mode);
		}catch(FileNotFoundException e){
			file = getFileIntChache(file, create);
			if(file==null)
				return null;
			try {
				rand = new RandomAccessFile(file, mode);
			} catch (FileNotFoundException e1) {
				return null;
			}
		}

		pair = new CharonPair<RandomAccessFile, FileChannel>(rand, rand.getChannel());

		channels.put(fileId, pair);
		return pair;
	}

	private File getFileIntChache(File file, boolean create)  {
		File rootCache = new File(cache);
		while(!rootCache.exists())
			rootCache.mkdirs();
		File data = new File(cacheDir);
		while(!data.exists())
			data.mkdirs();
		while(!file.exists()){
			if(create)
				try {
					file.createNewFile();
				} catch (IOException e) {
					e.printStackTrace();
				}
			else
				return null;
		}

		return file;
	}


	private void freeSomeMemory(long len) throws IOException{
		File data = new File(cacheDir);
		File diskRoot = new File("/");
		while(diskRoot.getFreeSpace() <= len){
			File[] files = data.listFiles();
			if(files.length == 0)
				return;
			long minimumTime = -1;
			long aux = 0;
			long lastMod = 0;
			long lastAcc = 0;
			File toDelete=null;
			for(int i = 0; i < files.length; i++){
				BasicFileAttributes attrs = Files.readAttributes(files[i].toPath(), BasicFileAttributes.class);
				lastMod = attrs.lastModifiedTime().toMillis();
				lastAcc = attrs.lastAccessTime().toMillis();
				if(minimumTime == -1)
					minimumTime = lastMod;

				if(lastAcc > lastMod)
					aux = lastMod;
				else
					aux = lastAcc;

				if(aux < minimumTime){
					minimumTime = aux;
					toDelete=files[i];
				}
			}
			if(toDelete!=null)
				toDelete.delete();
		}
	}


	//	public int read(String fileId, int offset, int capacity,  byte[] buf, int index) {
	//
	//
	//		try {
	//			File rootCache = new File(cache);
	//			while(!rootCache.exists())
	//				rootCache.mkdirs();
	//			File data = new File(cacheDir);
	//			while(!data.exists())
	//				data.mkdirs();
	//			File file = new File(cacheDir+fileId);
	//			if(!file.exists()){
	//				return -1;
	//			}
	//
	//			RandomAccessFile rand = new RandomAccessFile(file, "r");
	//			rand.seek(offset);
	//			rand.read(buf, index, capacity);
	//			rand.close();
	//
	//			return 0;
	//		} catch (IOException e) {
	//			e.printStackTrace();
	//		}
	//		return -1;
	//	}

}
