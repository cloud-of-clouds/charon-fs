package charon.storageService.externalSendThread;

import depsky.util.Pair;

public class ExternalObjectToSend {

	private String destFileName;
	private Pair<String, Long> blocksOffset;
	private String flushId;

	public ExternalObjectToSend(String destFileName, Pair<String, Long> blocksOffset, String flushId) {
		this.destFileName = destFileName;
		this.blocksOffset = blocksOffset;
		this.flushId = flushId;
	}

	public Pair<String, Long> getBlocksOffset() {
		return blocksOffset;
	}
	
	public String getDestFileName() {
		return destFileName;
	}
	
	public String getFlushId() {
		return flushId;
	}
	
}
