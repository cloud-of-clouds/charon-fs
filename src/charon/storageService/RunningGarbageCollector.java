package charon.storageService;

import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import charon.configuration.CharonConfiguration;
import charon.storageService.accessors.AmazonAcessor;
import charon.storageService.accessors.DepSkyAcessor;
import charon.storageService.accessors.PrivateRepositoryAcessor;

public class RunningGarbageCollector extends Thread{

	private DepSkyAcessor depsky;
	private AmazonAcessor amazon;
	private PrivateRepositoryAcessor locaRep;
	private ConcurrentHashMap<String,GarbageCollectorObject> garbageObjects;


	public RunningGarbageCollector(int clientId, ConcurrentHashMap<String,GarbageCollectorObject> garbageObjects, CharonConfiguration config, DepSkyAcessor accessor, PrivateRepositoryAcessor localRep){

		this.depsky = accessor;
		//this.amazon = new AmazonAcessor("", "", config);
		this.locaRep = localRep;
		this.garbageObjects = garbageObjects;
	}

	public void run(){

		for(Entry<String, GarbageCollectorObject> go : garbageObjects.entrySet()){
			switch (go.getValue().getLocation()) {
			case CoC:
				for(Entry<String, Integer> nv : go.getValue().getListOfVersionsPerBlock().entrySet()){
					if(go.getValue().getIsDeleteFile())
						depsky.delete(nv.getKey());
					else
						depsky.garbageCollection(nv.getKey(), GarbageToCollect.numOfVersionsToKeep);
				}
				break;
			case SINGLE_CLOUD:
				for(Entry<String, LinkedList<String>> nv : go.getValue().getListOfHashsPerBlock().entrySet()){
					for(int i = 0; i < nv.getValue().size(); i++){
						amazon.delete(nv.getKey(), nv.getValue().get(i));
					}
				}
				break;
			case PRIVATE_REP:
				for(Entry<String, LinkedList<String>> nv : go.getValue().getListOfHashsPerBlock().entrySet()){
					for(int i = 0; i < nv.getValue().size(); i++){
						locaRep.delete(nv.getKey(), nv.getValue().get(i));
					}
				}
				break;
			default:
				break;
			}
		}

	}
}
