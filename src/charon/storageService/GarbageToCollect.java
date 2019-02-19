package charon.storageService;

import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import charon.configuration.CharonConfiguration;
import charon.configuration.Location;
import charon.configuration.storage.cloud.SingleCloudConfiguration;
import charon.directoryService.DirectoryServiceImpl;
import charon.storageService.accessors.AmazonAcessor;
import charon.storageService.accessors.DepSkyAcessor;
import charon.storageService.accessors.PrivateRepositoryAcessor;

public class GarbageToCollect {

	private final int numOfVersionsToActiveGC = 10;
	private final int numOfBytesToActiveGC = 100;

	public static final int numOfVersionsToKeep = 3;

	private int versionsCount;
	private long bytesCount;

	private int clientId;
	private ConcurrentHashMap<String,GarbageCollectorObject> garbageObjects;
	private CharonConfiguration config;
	private DepSkyAcessor depSkyAccessor;
	private PrivateRepositoryAcessor localRep;
	private AmazonAcessor amazonAccessor;


	public GarbageToCollect(int clientId, CharonConfiguration config, DepSkyAcessor acessor, DirectoryServiceImpl diS){
		this.config = config;
		this.clientId = clientId;
		this.garbageObjects = new ConcurrentHashMap<String,GarbageCollectorObject>();
		this.depSkyAccessor = acessor;
		this.localRep = new PrivateRepositoryAcessor(config, diS);
		SingleCloudConfiguration oneCloudConfig = config.getSingleCloudConfig();
		if(oneCloudConfig!=null)
			this.amazonAccessor = new AmazonAcessor(clientId, oneCloudConfig.getAccessKey(), oneCloudConfig.getSecretKey(), config);
	}

	public void addNewVersionToCollect(String blockId, Location location, int size, String hash){
		String fileId = StorageService.getFileIdFromBlockId(blockId);
		if(!garbageObjects.contains(fileId)){
			garbageObjects.put(fileId, new GarbageCollectorObject(fileId, location, false));
		}
		GarbageCollectorObject garbageObject = garbageObjects.get(fileId);
		garbageObject.addVersion(blockId, hash);

		if(garbageObject.getNumOfVersionPerBlockId(blockId) > numOfVersionsToKeep){
			versionsCount++;
			bytesCount += size;
		}
		if(versionsCount > numOfVersionsToActiveGC || bytesCount > numOfBytesToActiveGC)
			activeGarbageCollector();
	}

	//TODO:a informacao de que o ficheiro foi apagado tem que chegar ate aos outros users que tem tb accesso ao ficheiro (LOCAL_REP e AMAZON)
	public void addNewFileToCollect(String fileId, int numOfBlocks, Location location){

		if(!garbageObjects.contains(fileId)){
			garbageObjects.put(fileId, new GarbageCollectorObject(fileId, location, true));
		}
		GarbageCollectorObject garbageObject = garbageObjects.get(fileId);
		garbageObject.setIsDeleteFile();
		garbageObject.setNumOfBlocks(numOfBlocks);

	}

	private void activeGarbageCollector(){

		ConcurrentHashMap<String, GarbageCollectorObject> toGarbageObjects = new ConcurrentHashMap<String, GarbageCollectorObject>();
		GarbageCollectorObject newObj = null;
		LinkedList<String> hashsToDelete = null;
		//corre o ficheiros
		for(Entry<String, GarbageCollectorObject> go : garbageObjects.entrySet()){
			if(!go.getValue().getIsDeleteFile()){
				newObj = new GarbageCollectorObject(go.getKey(), go.getValue().getLocation(), go.getValue().getIsDeleteFile());
				//corre os blocos
				boolean flag = false;
				for(Entry<String, Integer> nv : go.getValue().getListOfVersionsPerBlock().entrySet()){
					hashsToDelete = go.getValue().getListOfVersionToDeletePerBlockId(nv.getKey());
					if(hashsToDelete.size() > 0){
						flag = true;
						for(int i = 0; i < hashsToDelete.size(); i++){
							newObj.addVersion(nv.getKey(), hashsToDelete.get(i));
						}
					}
				}
				if(flag)
					toGarbageObjects.put(go.getKey(), newObj);
			}else{
				toGarbageObjects.put(go.getKey(), go.getValue());
				garbageObjects.remove(go.getKey());
			}
		}

		new RunningGarbageCollector(clientId, toGarbageObjects, config, depSkyAccessor, localRep).start();
	}

	private void writeToDisk(){

	}
}
