package charon.storageService;
import java.nio.ByteBuffer;
import java.util.Arrays;

import charon.util.CharonPair;
import charon.util.MapWVersionControl;


public class MemoryCacheManager implements ICacheManager{

	private long maxMemorySize;
	private MapWVersionControl<String, CharonPair<Integer,byte[]>> memory;
	private DiskCacheManager diskManager;

	public MemoryCacheManager(int memorySize, DiskCacheManager diskManager){
		this.memory = new MapWVersionControl<String, CharonPair<Integer,byte[]>>();
		this.diskManager = diskManager;
		this.maxMemorySize = (long) (getFreeMem()*0.4);
		System.out.println("MAX Main memory usage: " + maxMemorySize);
	}




	public int read(String fileId, ByteBuffer buf, int offset, int capacity ) {
		CharonPair<Integer, byte[]> data = memory.get(fileId);
		if(data==null)
			return -1;

		buf.put(data.getV(), offset, capacity);

		return 0;
	}

	@Override
	public byte[] readWhole(String fileId) {
		CharonPair<Integer, byte[]> data = memory.get(fileId);
		if(data==null)
			return null;

		if(data.getK() == data.getV().length)
			return data.getV();

		byte[] result = Arrays.copyOf(data.getV(), data.getK());

		return result;
	}



	@Override
	public int write(String fileId, ByteBuffer buf,	int dstOffset, int dataLen) {
		if(dstOffset+dataLen > maxMemorySize)
			return diskManager.write(fileId, buf, dstOffset, dataLen);

		CharonPair<Integer, byte[]> data = null;

		synchronized (fileId.intern()) {
			data = memory.get(fileId);
			if(data==null){

				//				// ====== MEMORY MANAGEMENT =========
				while(memory.getMemoryUsed() > 0 && memory.getMemoryUsed() + ((dstOffset+dataLen)*2) > maxMemorySize)
					freeSomeMemory();
				//				// =================================

				byte[] value = new byte[dstOffset+dataLen];
				buf.get(value, dstOffset, dataLen);
				memory.put(fileId, new CharonPair<Integer, byte[]>(value.length, value));
			}else{
				byte[] dataNew = null;
				if(dstOffset+dataLen > data.getV().length){
					dataNew = Arrays.copyOf(data.getV(), (dstOffset+dataLen)*2);
				}else{
					dataNew = data.getV();
				}
				buf.get(dataNew, dstOffset, dataLen);
				memory.put(fileId, new CharonPair<Integer, byte[]>(dstOffset+dataLen > data.getK() ? dstOffset+dataLen : data.getK(), dataNew));
			}
		}
		return 0;
	}

	@Override
	public int writeWhole(String fileId, byte[] value) {
		while(memory.getMemoryUsed() + (value.length*2) > maxMemorySize){
			freeSomeMemory();
		}

		CharonPair<Integer, byte[]> data = null;

		synchronized (fileId.intern()) {
			data = memory.get(fileId);
			if(data==null){
				memory.put(fileId, new CharonPair<Integer, byte[]>(value.length, value));
			}else{
				byte[] dataNew = null;
				if(value.length > data.getV().length){
					dataNew = Arrays.copyOf(data.getV(), (value.length)*2);
				}else{
					dataNew = data.getV();
				}
				for(int i = 0; i < value.length; i++){
					dataNew[i] = value[i];
				}
				//UPDATE THE ACTUAL SIZE
				memory.put(fileId,new CharonPair<Integer, byte[]>(value.length > data.getK() ? value.length : data.getK(), dataNew));
			}
		}
		return 0;
	}

	public int truncate(String fileId, int size) {
		synchronized (fileId.intern()) {
			CharonPair<Integer, byte[]> data = memory.get(fileId);
			if(data!=null){
				memory.put(fileId, new CharonPair<Integer, byte[]>(size, Arrays.copyOf(data.getV(), size)) );
			}
		}
		return 0;
	}


	public int delete(String fileId) {
		synchronized (fileId.intern()) {
			memory.discard(fileId);
		}
		return 0;
	}


	public boolean isInCache(String fileId) {
		synchronized (fileId.intern()) {
			return memory.contains(fileId);
		}
	}

	private long getFreeMem(){
		long allocatedMemory = (Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory());
		long presumableFreeMemory = Runtime.getRuntime().maxMemory() - allocatedMemory;
		return presumableFreeMemory;
	}

	private void freeSomeMemory(){
		System.out.println("REALEASING MEMORY! - " + memory.getMemoryUsed());
		CharonPair<String, CharonPair<Integer, byte[]>> discarded = memory.discardOldest();

		if(discarded==null || discarded.getV()==null || discarded.getK() == null)
			return;

		CharonPair<Integer, byte[]> data = discarded.getV();

		diskManager.writeWhole(discarded.getK(), data.getV(), data.getK());

		System.out.println("Memory ocupied LRU = " + memory.getMemoryUsed());
		return;
	}

	//	/**
	//	 * 
	//	 * @param fileId
	//	 * @param offset
	//	 * @param capacity
	//	 * @param buf
	//	 * @param index
	//	 * @requires buf!=null & capacity>=0 && index >=0
	//	 * @return
	//	 */
	//	public int read(String fileId, int offset, int capacity, byte[] buf, int index) {
	//		CharonPair<Integer, byte[]> data = memory.get(fileId);
	//		if(data==null)
	//			return -1;
	//
	//		//		if(capacity==-1){
	//		//			capacity=data.getK();
	//		//			if(buf==null){
	//		//				buf = new byte[capacity];
	//		//			}
	//		//		}
	//		byte[] dataArray = data.getV();
	//		for(int i = offset; i < offset + capacity; i++){
	//			//			if(i > data.getK())
	//			//				break;
	//			buf[index++] = dataArray[i];
	//		}
	//		
	//		byte[] cloneArray = Arrays.copyOf(buf, buf.length);
	//		diskManager.read(fileId+diff, offset, capacity, cloneArray, index);
	//		
	//		if(!equalsArrays(buf,  cloneArray, 0, index+capacity)){
	//			System.out.println("=========");
	//			System.out.println("NOT EQUALS ARRAYS...");
	//			System.out.println("fileId = " + fileId);
	//			System.out.println("offset = " + offset);
	//			System.out.println("=========");
	//		}
	//		return 0;
	//	}

}
