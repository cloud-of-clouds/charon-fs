package charon.storageService;

import charon.configuration.Location;
import depsky.client.messages.metadata.ExternalMetadata;


public class ObjectInQueue {

	private String fileId;
	private byte[] data;
	private Location location;
	private ExternalMetadata versionInfo;
	private String flushId;

	public ObjectInQueue(String fileId, byte[] data, Location location, ExternalMetadata verionInfo, String flushId){
		this.fileId = fileId;
		this.location = location;
		this.data = data;
		this.versionInfo = verionInfo;
		this.flushId = flushId;
	}
	
	public String getFileId(){
		return fileId;
	}
	
	public ExternalMetadata getVersionInfo() {
		return versionInfo;
	}

	public String getNSId(){
		String[] split = fileId.split("#");
		if(split.length >=2)
			return split[0];
		return null;
	}
	
	public Location getLocation() {
		return location;
	}

	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data=data;		
	}
	
	public String getFlushId() {
		return flushId;
	}
}
