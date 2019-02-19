package charon.storageService.externalSendThread;

import java.util.concurrent.BlockingQueue;

import charon.configuration.Location;
import charon.storageService.StorageService;
import charon.storageService.accessors.ExternalRepositoryAccessor;
import charon.util.ExternalMetadataDummy;

public class ExternalBlocksSendThread extends Thread {

	private BlockingQueue<ExternalObjectToSend> queue;
	private ExternalRepositoryAccessor accessor;
	private StorageService sts;

	public ExternalBlocksSendThread(BlockingQueue<ExternalObjectToSend> queue, ExternalRepositoryAccessor externalAccessor, StorageService storageService) {
		this.queue = queue;
		this.accessor = externalAccessor;
		this.sts = storageService;
	}	


	@Override
	public void run() {
		while(true){
			try {
				ExternalObjectToSend obj = queue.take();
				String hash = accessor.write(obj);
				sts.commit(obj.getBlocksOffset().getKey(), new ExternalMetadataDummy(hash), Location.EXTERNAL_REP, obj.getFlushId());
			} catch (InterruptedException e) {}
		}
	}


}
