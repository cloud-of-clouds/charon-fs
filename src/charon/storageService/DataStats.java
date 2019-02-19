package charon.storageService;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

import charon.util.ExternalMetadataDummy;
import depsky.client.messages.metadata.ExternalMetadata;


public class DataStats implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 6900093748194532598L;
	private String fileId;
	
	private ConcurrentHashMap<Integer, Boolean> wasBlockFlushed;
	private ConcurrentHashMap<Integer, Boolean> writeInCloudsPerBlock;
	private ConcurrentHashMap<Integer, Boolean> writeInDiskPerBlock;
	private ConcurrentHashMap<Integer, ExternalMetadata> hashs;
	private ExternalMetadata externalFileHash;
	private int sizeOfLastBlock;
	private int numberOfBlocks;
	private long totalSize;

	public DataStats(String fileId){		
		this.fileId = fileId;
		this.wasBlockFlushed = new ConcurrentHashMap<Integer, Boolean>();
		this.writeInCloudsPerBlock = new ConcurrentHashMap<Integer, Boolean>();
		this.writeInDiskPerBlock = new ConcurrentHashMap<Integer, Boolean>();
		this.hashs = new ConcurrentHashMap<Integer, ExternalMetadata>();
		this.sizeOfLastBlock = 0;
		this.numberOfBlocks = 1;
		this.totalSize = 0;
		this.externalFileHash = null;
	}

	public String getFileId(){
		return this.fileId;
	}

	public boolean getWriteInClouds(int blockNumber){
		if(!this.writeInCloudsPerBlock.containsKey(blockNumber))
			return false;
		return this.writeInCloudsPerBlock.get(blockNumber);
	}
	
	public boolean getWasBlockFlushed(int blockNumber){
		if(!this.wasBlockFlushed.containsKey(blockNumber))
			return false;
		return this.wasBlockFlushed.get(blockNumber);
	}

	public ConcurrentHashMap<Integer, Boolean> getAllBlockFlushed(){
		return this.wasBlockFlushed;
	}

	public ConcurrentHashMap<Integer, Boolean> getAllWriteInCloudsValues(){
		return this.writeInCloudsPerBlock;
	}
	
	public boolean getWriteInDisk(int blockNumber){
		if(!this.writeInDiskPerBlock.containsKey(blockNumber))
			return false;
		return this.writeInDiskPerBlock.get(blockNumber);
	}

	public ConcurrentHashMap<Integer, Boolean> getAllWriteInDiskValues(){
		return this.writeInDiskPerBlock;	
	}

	public ExternalMetadata getHash(int blockNumber){
		return this.hashs.get(blockNumber);
	}

	public int getSize(){
		return this.sizeOfLastBlock;
	}

	public long getTotalSize(){
		return this.totalSize;
	}

	public int getNumberOfBlocks(){
		return this.numberOfBlocks;
	}

	public void setWriteInClouds(boolean writeInClodus, int blockNumber){
		this.writeInCloudsPerBlock.put(blockNumber, writeInClodus);
	}
	
	public void setWasFlushed(boolean wasFlushed, int blockNumber){
		this.wasBlockFlushed.put(blockNumber, wasFlushed);
	}

	public void setWriteInDisk(boolean writeInDisk, int blockNumber){
		this.writeInDiskPerBlock.put(blockNumber, writeInDisk);
	}

	public void setHash(ExternalMetadata hash, int blockNumber){
		this.hashs.put(blockNumber, hash);
	}
	
	public void setHash(ExternalMetadata hash){
		this.externalFileHash = hash;
	}
	
	public ExternalMetadata getExternalFileHash(){
		return this.externalFileHash;
	}
	
	public void setSize(int size){
		this.sizeOfLastBlock = size;
	}

	public void setNumberOfBlocks(int numberOfBlocks) {
		this.numberOfBlocks = numberOfBlocks;
	}
	
	public void truncateInfo(int block){
		for(int i = block; i < numberOfBlocks; i++){
			hashs.remove(i);
			writeInCloudsPerBlock.remove(i);
			writeInDiskPerBlock.remove(i);
			wasBlockFlushed.remove(i);
		}
	}
	
	public void setTotalSize(long totalSize){
		this.totalSize = totalSize;
		int initialNumOfBlocks = this.numberOfBlocks;
		this.numberOfBlocks = ((int)(totalSize-1)/StorageService.blockSize) + 1;
		if(this.numberOfBlocks>initialNumOfBlocks)
			this.hashs.put(this.numberOfBlocks, new ExternalMetadataDummy(""));
//		if(((totalSize/StorageService.blockSize) + 1) > numberOfBlocks)
//			this.numberOfBlocks++;
	}
}
