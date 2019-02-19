package charon.directoryService;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.TimerTask;

import charon.general.NSAccessInfo;
import charon.general.Printer;
import charon.storageService.StorageService;
import charon.storageService.accessors.DepSkyAcessor;
import charon.storageService.accessors.IMetadataAccessor;
import charon.util.IOUtil;
import depsky.util.Pair;
import depsky.util.integrity.IntegrityManager;

public class NSUpdaterTimerTask extends TimerTask{

	private NameSpace lastUpdate;
	private boolean isScheduled;
	private IMetadataAccessor accessor;
	private String pathId;
	private NameSpace toSend;
	private List<Pair<String, NSAccessInfo>> snss;
	private boolean isSNS;



	public NSUpdaterTimerTask(IMetadataAccessor accessor, String pathId, boolean isSNS) {
		this.isScheduled = false;
		this.accessor = accessor;
		this.pathId = pathId;
		this.isSNS = isSNS;
	}

	public boolean isScheduled() {
		return isScheduled;
	}

	public void schedule(){
		isScheduled = true;
	}

	public void setLastUpdate(NameSpace lastUpdate) {
		synchronized (pathId) {
			this.lastUpdate = lastUpdate;
		}
	}

	public void setSNS(List<Pair<String,NSAccessInfo>> s){
		this.snss=s;
	}

	@Override
	public void run() {

		while(lastUpdate != null){
			synchronized (pathId) {
				toSend = lastUpdate;
				lastUpdate = null;
			}

			try{
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(baos);

				if(snss!=null){
					oos.writeInt(snss.size());
					for(Pair<String,NSAccessInfo> ns : snss){
						if(!ns.getKey().equals(pathId)){
							oos.writeUTF(ns.getKey());
							oos.writeObject(ns.getValue());
						}
					}
					oos.writeUTF(pathId);
				}


				toSend.writeExternal(oos);

				byte[] bagArray = baos.toByteArray();				
				if(isSNS){
					IOUtil.closeStream(oos);
					IOUtil.closeStream(baos);
					NameSpaceRepresentation nsRep = new NameSpaceRepresentation(toSend.getVersion(), IntegrityManager.getHexHash(bagArray), bagArray);
					baos = new ByteArrayOutputStream();
					oos = new ObjectOutputStream(baos);
					
					nsRep.writeExternal(oos);
					bagArray = baos.toByteArray();
				}

				IOUtil.closeStream(oos);
				IOUtil.closeStream(baos);

				Printer.println("NS Updater : sending NS.", "azul");
                                accessor.writeNS(pathId + "#" + pathId, bagArray);
//				accessor.directWrite(pathId + "#" + pathId, bagArray, true);
			}catch(Exception e){
				e.printStackTrace();
			}
		}

		synchronized (pathId) {
			if(lastUpdate==null)
				isScheduled = false;
			else
				this.run();
		}
	}
}
