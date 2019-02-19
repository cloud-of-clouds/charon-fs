package charon.storageService.repositories;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.net.URISyntaxException;

import javax.swing.filechooser.FileNameExtensionFilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import charon.storageService.StorageService;

public class HopsFSRepository implements FileRepository {

	private Path folder;
	private DistributedFileSystem fs;
	private String localFolderToTempFiles;
	private String cache;
	private String charonRepName;

	public HopsFSRepository(DistributedFileSystem fs, String charonRepName, String cache) {
		
			this.cache = (cache.endsWith(File.separator) ? cache : cache + File.separator);
			this.localFolderToTempFiles = this.cache + "hdfs temp files" + File.separator;
			File f = new File(localFolderToTempFiles);
			if(!f.exists() )
				while(!f.mkdirs());

			this.charonRepName = charonRepName+File.separator;
			this.fs = fs;

			this.folder = new Path(this.charonRepName);

			try {
				if(!fs.exists(folder)){
					fs.mkdirs(folder);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}

	}

	@Override
	public byte[] read(String filename) {
		synchronized (filename.intern()) {
			try {
				Path fileToRead = new Path(charonRepName+filename);
				if(fs.exists(fileToRead)){
					byte[] data = new byte[(int)fs.getFileLinkStatus(fileToRead).getLen()];
					FSDataInputStream input = fs.open(fileToRead);
					input.read(data, 0, data.length);
					return data;
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	@Override
	public long write(String filename, byte[] data) {
		synchronized (filename.intern()) {
			try {
				Path fileToWrite = new Path(charonRepName+filename);
				if(!fs.exists(fileToWrite))
					fs.createNewFile(fileToWrite);

				FSDataOutputStream out = fs.create(fileToWrite);
				out.write(data);
				out.close();
				return 1;

			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return -1;
	}

	@Override
	public long write(String filename, byte[] data, long offset) {
		synchronized (filename.intern()) {


			File f = new File(localFolderToTempFiles);
			if(!f.exists() )
				while(!f.mkdirs());

			try {
				Path fileToWrite = new Path(charonRepName+filename);

				if(!fs.exists(fileToWrite) && offset > 0)
					return -1;

				if(!fs.exists(fileToWrite)){
					FSDataOutputStream out = fs.create(fileToWrite);
					out.write(data);
					out.close();
					return 1;
				}else{
					
					String[] aux = filename.split("/");
					String file = aux[aux.length - 1];
					Path destination = new Path(localFolderToTempFiles+file);
					fs.copyToLocalFile(fileToWrite, destination);
					new File(localFolderToTempFiles+"."+file+".crc").delete();
					File fileToWrite_temp = new File(localFolderToTempFiles+file);
					RandomAccessFile rand = new RandomAccessFile(fileToWrite_temp, "rw");
					rand.seek(offset);
					rand.write(data);
					rand.close();
					fs.copyFromLocalFile(true, true, destination, fileToWrite);
					
//old code to write in a specific offset to an existent hdfs files					
//					long fileSize = fs.getFileStatus(fileToWrite).getLen();
//					int numOfBlocks = 0;
//					if(offset + data.length > fileSize){
//						numOfBlocks = StorageService.getBlockNumber(offset + data.length -1);
//					}else{
//						numOfBlocks = StorageService.getBlockNumber((fileSize - 1));
//					}
//					int blockToWrite = StorageService.getBlockNumber(offset);
//					String[] aux = filename.split("/");
//					String file = aux[aux.length - 1];
//					File fileToWrite_temp = new File(localFolderToTempFiles+file);
//					if(!fileToWrite_temp.exists())
//						fileToWrite_temp.createNewFile();
//					long calculated_offset = 0;
//					RandomAccessFile rand = new RandomAccessFile(fileToWrite_temp, "rw");
//					for(int i = 0; i < numOfBlocks; i++){
//						calculated_offset = i * StorageService.blockSize;
//						rand.seek(calculated_offset);
//						if(i+1 == blockToWrite){
//							rand.write(data);
//						}else{
//							byte[] value = read(filename, calculated_offset, StorageService.blockSize);
//							rand.write(value);
//						}
//					}
//
//					rand.seek(0);
//					byte[] allData = new byte[(int)fileToWrite_temp.length()];
//					rand.read(allData);
//					write(filename+"_temp", allData);
//					allData = null;
//					fs.delete(fileToWrite, true);
//					Path fileToWrite_re = new Path(charonRepName+filename+"_temp");
//					fs.rename(fileToWrite_re, fileToWrite);
//					fileToWrite_temp.delete();
//					rand.close();
					
				}
				return 1;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return 0;
	}

	@Override
	public byte[] read(String filename, long offset, int blocksize) {
		synchronized (filename.intern()) {
			try {

				Path fileToRead = new Path(charonRepName+filename);
				if(fs.exists(fileToRead)){
					long fileSize = fs.getFileStatus(fileToRead).getLen();
					if(StorageService.getBlockNumber(offset) == StorageService.getBlockNumber(fileSize-1)){
						blocksize = (int) (fileSize - offset);
					}

					byte[] data = new byte[blocksize];
					FSDataInputStream input = fs.open(fileToRead);
					input.seek(offset);
					int count = 0;
					while(count < blocksize){
						count += input.read(data, count, data.length-count);
					}
					input.close();
					return data;
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	@Override
	public void truncate(String filename, long newSize) {
		synchronized (filename.intern()) {
			try {
				Path fileToTrunc = new Path(charonRepName+filename);
				fs.truncate(fileToTrunc, newSize);
			} catch (IOException e) {
				e.printStackTrace();
			}		
		}
	}

	@Override
	public byte[] read(String filename, long offset, int blocksize,
			String externalManaged) {
		synchronized (filename.intern()) {
			try {

				Path fileToRead = new Path(externalManaged);
				if(fs.exists(fileToRead)){
					long fileSize = fs.getFileStatus(fileToRead).getLen();
					if(StorageService.getBlockNumber(offset) == StorageService.getBlockNumber(fileSize-1)){
						blocksize = (int) (fileSize - offset);
					}

					System.out.println("offset -> " + offset);
					System.out.println("blocksize -> " + blocksize);
					byte[] data = new byte[blocksize];
					FSDataInputStream input = fs.open(fileToRead);
					input.seek(offset);
					input.read(data, 0, data.length);
					return data;
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			return null;
		}

	}
}