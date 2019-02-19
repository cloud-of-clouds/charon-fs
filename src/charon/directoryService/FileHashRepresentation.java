package charon.directoryService;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import charon.util.ExternalMetadataDummy;
import depsky.client.messages.metadata.ExternalMetadata;

public class FileHashRepresentation implements Externalizable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 5524445180301100132L;
	private String pathId;
	private long fileSize;
	private Map<Integer, ExternalMetadata> hashes;

	
	public FileHashRepresentation(String pathId, long size, Map<Integer, ExternalMetadata> hashes) {
		this.pathId = pathId;
		this.fileSize = size;
		this.hashes = hashes;
	}
	
	public FileHashRepresentation() {
	}


	public String getPathId() {
		return pathId;
	}


	public void setPathId(String pathId) {
		this.pathId = pathId;
	}


	public long getFileSize() {
		return fileSize;
	}


	public void setFileSize(long fileSize) {
		this.fileSize = fileSize;
	}


	public Map<Integer, ExternalMetadata> getHashes() {
		return hashes;
	}


	public void setHashes(Map<Integer, ExternalMetadata> hashes) {
		this.hashes = hashes;
	}


	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		this.pathId = in.readUTF();
		this.fileSize = in.readLong();
		
		int size = in.readInt();
		if(size==-1){
			hashes = null;
		}else{
			hashes = new ConcurrentHashMap<Integer, ExternalMetadata>();
			int key;
			ExternalMetadata value;
			int len;
			for(int i = 0 ; i<size ; i++){
				key = in.readInt();
				len = in.readInt();
				if(len == -1){
					value = null;
				}else{
					//is Dummy?
					if(in.readBoolean())
						value = new ExternalMetadataDummy();
					else
						value = new ExternalMetadata();
					
					value.readExternal(in);
				}
				hashes.put(key, value);
			}
		}
		
	}


	@Override
	public void writeExternal(ObjectOutput out) throws IOException {		
		out.writeUTF(pathId);
		out.writeLong(fileSize);
		if(hashes == null){
			out.writeInt(-1);
		}else{
			out.writeInt(hashes.size());
			for(Entry<Integer, ExternalMetadata> e : hashes.entrySet()){
				out.writeInt(e.getKey());
				if(e.getValue()==null){
					out.write(-1);
				}else{
					out.writeInt(0);
					out.writeBoolean(e.getValue() instanceof ExternalMetadataDummy);
					e.getValue().writeExternal(out);;
				}
			}
		}
		out.flush();
	}
	
}
