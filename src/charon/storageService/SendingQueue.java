package charon.storageService;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import charon.configuration.CharonConfiguration;
import charon.configuration.Location;
import depsky.client.messages.metadata.ExternalMetadata;

public class SendingQueue {

	private ConcurrentHashMap<Integer, SendingThread> sendingThreads;
	private ConcurrentHashMap<Integer, Boolean> activeThreads;
	private ConcurrentHashMap<String, ObjectInQueue> objectsToSend;
	private ConcurrentLinkedQueue<String> queue;
	private ConcurrentHashMap<Integer, String> activeObjects;



	public SendingQueue(StorageService daS, int numOfThreads, int clientId, CharonConfiguration config){
		this.sendingThreads = new ConcurrentHashMap<Integer, SendingThread>();
		this.objectsToSend = new ConcurrentHashMap<String, ObjectInQueue>();
		this.activeThreads = new ConcurrentHashMap<Integer, Boolean>();
		this.queue = new ConcurrentLinkedQueue<String>();
		this.activeObjects = new ConcurrentHashMap<Integer, String>();

		System.out.println("num of threads = " + numOfThreads);
		
		for(int i = 0; i < numOfThreads; i++){
			sendingThreads.put(i, new SendingThread(daS, this, clientId, i, config));
			sendingThreads.get(i).start();
			activeThreads.put(i, true);
		}		

	}
	//return true if the block is in queue and will be replaced
	public boolean addSendingObject(String fileId, Location location, byte[] data, ExternalMetadata versionInfo, String flushId){
		ObjectInQueue obj = null;
		//se esta na fila
		if(queue.contains(fileId)){
			obj = objectsToSend.get(fileId);
			obj.setData(data);
			objectsToSend.put(fileId, obj);
			return false;
		}else{
			obj = new ObjectInQueue(fileId, data, location, versionInfo, flushId);
			boolean flag = false;
			//se esta a ser enviado
//			if(activeObjects.containsValue(fileId)){
//				Enumeration<Integer> e = activeObjects.keys();
//				int elemKey;
//				while(e.hasMoreElements()){
//					elemKey = e.nextElement();
//					if(activeObjects.get(elemKey).equals(fileId)){
//						flag = sendingThreads.get(elemKey).setSameObjectToSend(obj);
//					}
//				}
//			}
			//nao esta em fila nem a ser enviado
//			if(!flag){
//				flag = false;
				for(int i = 0; i < sendingThreads.size(); i++){
					if(activeThreads.get(i)){
						activeThreads.put(i, false);
						sendingThreads.get(i).setObjectToSend(obj);
						activeObjects.put(i, fileId);
						i = sendingThreads.size() + 1;
						flag = true;
					}
				}
				if(!flag){
					objectsToSend.put(fileId, obj);
					queue.add(fileId);
				}
//			}
				return true;	
		}
	}

	public void removeSendingObject(String fileId, int numOfBlocks){

		String blockId;
		for(int i = 1; i <= numOfBlocks; i++){
			blockId = fileId.concat("_".concat(i+""));
			if(queue.contains(blockId)){
				queue.remove(blockId);
				//loS.release(fileId);
				objectsToSend.remove(blockId);
			}
		}
	}

	public boolean releaseSendingThread(int threadId){
		if(queue.size() > 0){
			String fileId = queue.poll();
			
			//FIXME: (ricardo) isto da null pointer na linha 100 de vez em quando com o postmark. Pus este if. é suposto retornar true ou false???
			if(fileId == null)
				return true;
			
			ObjectInQueue objToSend = objectsToSend.get(fileId);
			SendingThread thread = sendingThreads.get(threadId);
			if(thread!=null && objToSend!=null)
				thread.setObjectToSend(objToSend);
			activeObjects.put(threadId, fileId);
			objectsToSend.remove(fileId);
			return true;
		}else{
			activeThreads.put(threadId, true);
			activeObjects.remove(threadId);
			return false;
		}
	}

	public void releaseSendigObject(String fileId, boolean toRelease){


		//if(queue.contains(fileId)){
		//		if(fileId != null){
		//			ObjectInQueue obj = objectsToSend.get(fileId);
		//			if(obj.getToRelease()){
		//				loS.release(fileId);
		//			}else{
		//				obj.setToRelease(toRelease);
		//				objectsToSend.put(fileId, obj);
		//			}
		//		}else{
		//			boolean flag = false;
		//			for(int i = 0; i < sendingThreads.size(); i++){
		//				if(!activeThreads.get(i)){
		//					flag = sendingThreads.get(i).setToRelease(fileId, toRelease);
		//					if(flag)
		//						i=sendingThreads.size()+1;
		//				}
		//			}
		//			//o envio do objecto já terminou e o release nao foi feito
		//			if(!flag){
		//				loS.release(fileId);
		//			}
		//		}
	}

}
