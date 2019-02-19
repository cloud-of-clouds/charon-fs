package charon.storageService;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.ConcurrentHashMap;

import charon.directoryService.DirectoryServiceImpl;
import charon.directoryService.NodeMetadata;
import charon.util.ExternalMetadataDummy;
import depsky.client.messages.metadata.ExternalMetadata;


public class DataStatsManager {

	private ConcurrentHashMap<String, DataStats> dataStats;
	private String infoDir;
	private String cache;

	public DataStatsManager(String cache){

		this.dataStats = new ConcurrentHashMap<String, DataStats>();
		this.cache = cache+ File.separator;
		this.infoDir = this.cache + "info directory" + File.separator;
		File f = new File(infoDir);
		if(!f.exists()){
			while(!f.mkdirs());
		}
	}

	public void recover(DirectoryServiceImpl diS, SendingQueue queue){

		NodeMetadata fileMeta;
		File file = new File(infoDir);
		if(file.isDirectory()){
			String[] infoFiles = file.list();
			DataStats dt = null;
			for(int i = 0; i < infoFiles.length; i++){
				dt = readInfoFromDisk(infoFiles[i]);
				ConcurrentHashMap<Integer, Boolean> allBlocksFlushed = dt.getAllBlockFlushed();
				fileMeta = null;
				for(int j = 1; j <= allBlocksFlushed.size(); j++){
					if(allBlocksFlushed.get(j)){
						if(fileMeta == null)
							fileMeta = diS.getMetadataByPathId(dt.getFileId());
						if(fileMeta != null && (diS.getMetadataByPathId(dt.getFileId()).getDataHashList().get(j) == null || dt.getHash(j).equals(fileMeta.getDataHashList().get(j)))){
							queue.addSendingObject(dt.getFileId().concat("_".concat(j+"")), fileMeta.getLocation(), null, fileMeta.getDataHashList().get(j), "");
						}else{
							setWriteInClouds(dt.getFileId(), j, false);
						}
					}
				}
				dataStats.put(infoFiles[i], dt);
			}
		}
	}

	public DataStats getDataStats(String fileId){
		return dataStats.get(fileId);
	}

	public DataStats newDataStats(String fileId){
		DataStats dt = new DataStats(fileId);
		dt.setHash(new ExternalMetadataDummy(""), 1);
		dataStats.put(fileId, dt);
		return dt;
	}
//
//	public void setNumOfBlocks(String fileId, int numOfBlocks){
//
//		DataStats dt = dataStats.get(fileId);
//		dt.setNumberOfBlocks(numOfBlocks);
//		writeInfoToDisk(dt);
//
//	}

	public void setHash(String fileId, int blockNumber,ExternalMetadata hash){
		if(dataStats.containsKey(fileId)){
			DataStats dt = dataStats.get(fileId);
			dt.setHash(hash, blockNumber);
			writeInfoToDisk(dt);
		}else{
			//dizer que a versão transferida pela Thread de envio já foi eliminada ao garbage collector
		}
	}

	public void setWriteInClouds(String fileId, int blockNumber, boolean writeInclouds){
		if(dataStats.containsKey(fileId)){
			DataStats dt = dataStats.get(fileId);
			dt.setWriteInClouds(writeInclouds, blockNumber);
		}else{
			//dizer que a versão transferida pela Thread de envio já foi eliminada ao garbage collector
		}
	}

	public void setWasFlushed(String fileId, int blockNumber, boolean wasFlushed, boolean isToSaveInDisk){
		if(dataStats.containsKey(fileId)){
			DataStats dt = dataStats.get(fileId);
			dt.setWasFlushed(wasFlushed, blockNumber);
			if(isToSaveInDisk)
				writeInfoToDisk(dt);
		}else{
			//dizer que a versão transferida pela Thread de envio já foi eliminada ao garbage collector
		}
	}
	
	public void setWriteInDisk(String fileId, int blockNumber, boolean writeInDisk){
		if(dataStats.containsKey(fileId)){
			DataStats dt = dataStats.get(fileId);
			dt.setWriteInDisk(writeInDisk, blockNumber);
		}
	}
	
	public void truncateInfo(String fileId, int block){
		if(dataStats.containsKey(fileId)){
			DataStats dt = dataStats.get(fileId);
			dt.truncateInfo(block);
		}
	}

	public void setSize(String fileId, int size, boolean isToSaveInDisk){
		if(dataStats.containsKey(fileId)){
			DataStats dt = dataStats.get(fileId);
			dt.setSize(size);
			if(isToSaveInDisk)
				writeInfoToDisk(dt);
		}
	}

	public void setToTalSize(String fileId, long totalSize){
		DataStats dt = dataStats.get(fileId);
		dt.setTotalSize(totalSize);
	}

	public void deleteDataStats(String fileId){
		File file = new File(infoDir+fileId);
		if(file.exists()){
			file.delete();
		}
		dataStats.remove(fileId);
		//dizer que o ficheiro foi elimindado ao garbage collector
	}

	public void setNewVersionTranfered(String fileId, int numBytes){

	}

	private void writeInfoToDisk(DataStats dataStats){
		File rootCache = new File(cache);
		while(!rootCache.exists())
			rootCache.mkdirs();
		File info = new File(infoDir);
		while(!info.exists())
			info.mkdirs();
		try {
			FileOutputStream fout = new FileOutputStream(infoDir+dataStats.getFileId());
			ObjectOutputStream out = new ObjectOutputStream(fout);
			out.writeObject(dataStats);
			out.flush();
			out.close();
			fout.flush();
			fout.close();			

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private DataStats readInfoFromDisk(String fileId){
		File rootCache = new File(cache);
		while(!rootCache.exists())
			rootCache.mkdirs();
		File info = new File(infoDir);
		while(!info.exists())
			info.mkdirs();
		try{
			FileInputStream fin = new FileInputStream(infoDir+fileId);
			ObjectInputStream in = new ObjectInputStream(fin);
			DataStats dt = (DataStats) in.readObject();
			in.close();
			fin.close();
			return dt;
		}catch (IOException e) {
			e.printStackTrace();
		}catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}
}
