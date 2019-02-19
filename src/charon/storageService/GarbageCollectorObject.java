package charon.storageService;

import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

import charon.configuration.Location;

public class GarbageCollectorObject {

	private String fileId;
	private boolean isDeteleFile;
	private Location location;
	private int numOfBlocks;
	private ConcurrentHashMap<String, Integer> writtenVersions;
	private ConcurrentHashMap<String, LinkedList<String>> writtenHashs;

	public GarbageCollectorObject(String fileId, Location location, boolean isDeleteFile){
		this.fileId = fileId;
		this.isDeteleFile = isDeleteFile;
		this.location = location;
		this.numOfBlocks = 0;
		this.writtenVersions = new ConcurrentHashMap<String, Integer>();
		this.writtenHashs = new ConcurrentHashMap<String, LinkedList<String>>();
	}

	public void setIsDeleteFile(){
		this.isDeteleFile = true;
	}

	public void setNumOfBlocks(int numaOfBlocks){
		this.numOfBlocks = numaOfBlocks;
	}

	public void addVersion(String blockId, String hash){
		if(!writtenVersions.contains(blockId)){
			writtenVersions.put(blockId, 0);
			writtenHashs.put(blockId, new LinkedList<String>());
		}else{
			writtenVersions.put(blockId, writtenVersions.get(blockId)+1);
			if(location != Location.CoC)
				writtenHashs.get(blockId).addLast(hash);
		}
	}

	public int getNumOfVersionPerBlockId(String blockId){
		return this.writtenVersions.get(blockId);
	}

	public LinkedList<String> getHashsPerBlockID(String blockId){
		return this.writtenHashs.get(blockId);
	}

	public boolean getIsDeleteFile(){
		return this.isDeteleFile;
	}

	public Location getLocation(){
		return this.location;
	}

	public ConcurrentHashMap<String, Integer> getListOfVersionsPerBlock(){
		return this.writtenVersions;
	}
	
	public ConcurrentHashMap<String, LinkedList<String>> getListOfHashsPerBlock(){
		return this.writtenHashs;
	}

	//returns
	public LinkedList<String> getListOfVersionToDeletePerBlockId(String blockId){

		LinkedList<String> toRet = new LinkedList<String>();
		while(writtenHashs.get(blockId).size() > GarbageToCollect.numOfVersionsToKeep){
			toRet.add(writtenHashs.get(blockId).poll());
			writtenVersions.put(blockId, writtenVersions.get(blockId)-1);
		}
		return toRet;
	}


}
