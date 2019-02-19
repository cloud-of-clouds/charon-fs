package charon.storageService.accessors;

import java.io.UnsupportedEncodingException;

import charon.configuration.CharonConfiguration;
import charon.directoryService.DirectoryServiceImpl;
import charon.general.NSAccessInfo;
import charon.general.Printer;
import charon.storageService.DiskCacheManager;
import charon.storageService.StorageService;
import charon.storageService.externalSendThread.ExternalObjectToSend;
import charon.storageService.repositories.FileRepository;
import charon.storageService.repositories.FileRepositoryFactory;
import charon.storageService.scpSockets.SCPManager;
import depsky.util.Pair;
import depsky.util.integrity.IntegrityManager;

public class ExternalRepositoryAccessor {


	private DirectoryServiceImpl diS;
	private DiskCacheManager disk;
	private FileRepository rep;
	//	private boolean useCompression;
	private int clientId;

	public ExternalRepositoryAccessor(CharonConfiguration config, DirectoryServiceImpl diS){
		this.diS = diS;
		this.disk = new DiskCacheManager(config.getCacheDirectory());
		this.rep = FileRepositoryFactory.build(config.getExternalRepositoryDirectory(), config.getCacheDirectory()); 
		//		this.useCompression = config.useCompression();
		// ==============

		this.clientId = config.getClientId();

	}

	public byte[] read(String filename, long offset, String dataHashHex, String externalManaged){
		NSAccessInfo snsInfo = diS.getNS(filename).getSnsInfo();
		if(snsInfo == null)
			return null;

		int owner = snsInfo.getOwnerId();
		System.out.println("owner - " + owner );
		byte[] data = null;


		long tempo = System.currentTimeMillis();
		if(owner == clientId){

			int blocksize = StorageService.blockSize;

			if(externalManaged==null)
				data = rep.read(filename, offset, blocksize);
			else
				data = rep.read(filename, offset, blocksize, externalManaged);

		}else{
			String filenameNew = filename;
			data = SCPManager.remoteExternalRepRead(filenameNew, offset, dataHashHex, externalManaged, diS.getRemotePeer(owner));


			tempo = System.currentTimeMillis() - tempo;


			//		if(data!=null  && useCompression)
			//			data = DeflaterCompressionUtil.decompress(data);

			if(data!=null){
				System.out.println("AFTER READ data len -> " + data.length);
				String hash = IntegrityManager.getHexHash(data);
				try {
					System.out.println("**** READ HASH | data="+new String(data, "UTF-8") + ", hash = " + hash);
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("1 -> " + hash);
				System.out.println("2 -> " + dataHashHex);
				if(!hash.equals(dataHashHex)){
					System.out.println("HASH dá diferente");
					return null;
				}
			}
		}
		if(data==null)
			Printer.println("  -> ExtRep: read " +filename+" - NULL response. [took: " + Long.toString(tempo) + " ms].", "verde");
		else{
			Printer.println("  -> ExtRep: read " +filename+" - OK! [took: " + Long.toString(tempo) + " ms].", "verde");
		}

		return data;

	}

	public byte[] read(String filename, long offset, String dataHashHex){
		NSAccessInfo snsInfo = diS.getNS(filename).getSnsInfo();
		if(snsInfo == null)
			return null;

		int owner = snsInfo.getOwnerId();
		System.out.println("owner - " + owner );
		byte[] data = null;


		long tempo = System.currentTimeMillis();
		if(owner == clientId){

			int blocksize = StorageService.blockSize;

			data = rep.read(filename, offset, blocksize);
		}else{
			data = SCPManager.remoteExternalRepRead(filename, offset, dataHashHex, null, diS.getRemotePeer(owner));
		}

		tempo = System.currentTimeMillis() - tempo;


		//		if(data!=null  && useCompression)
		//			data = DeflaterCompressionUtil.decompress(data);

		if(data!=null){
			String hash = IntegrityManager.getHexHash(data);
//			try {
//				System.out.println("**** READ HASH | data="+new String(data, "UTF-8") + ", hash = " + hash);
//			} catch (UnsupportedEncodingException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//			System.out.println("1 -> " + hash);
//			System.out.println("2 -> " + dataHashHex);
			if(!hash.equals(dataHashHex)){
				System.out.println("HASH dá diferente");
				return null;
			}
		}

		if(data==null)
			Printer.println("  -> ExtRep: read " +filename+" - NULL response. [took: " + Long.toString(tempo) + " ms].", "verde");
		else{
			Printer.println("  -> ExtRep: read " +filename+" - OK! [took: " + Long.toString(tempo) + " ms].", "verde");
		}

		return data;

	}

	public String write(ExternalObjectToSend obj) {
		byte[] buf = null;

		Pair<String, Long> pair = obj.getBlocksOffset();
		String[] names = pair.getKey().split("#", 2);
		NSAccessInfo snsInfo = diS.getSNSAccessInfo(names[0]);
		if(snsInfo == null)
			return null;

		int owner = snsInfo.getOwnerId();
		buf = disk.readWhole(pair.getKey());
		if(buf == null){
			Printer.println(" BUF = NULL... -> ExtRep: write " + obj.getDestFileName() + ", bOff = " + obj.getBlocksOffset().getValue() +", bId = " + obj.getBlocksOffset().getKey() + " - NULL", "verde");
			return null;
		}

		//		if(useCompression)
		//			buf = DeflaterCompressionUtil.compress(buf);


		String hash = IntegrityManager.getHexHash(buf);
		long tempo = System.currentTimeMillis();
		//		try {
		//			System.out.println("**** WRITE HASH | data="+new String(buf, "UTF-8") + ", hash = " + hash);
		//		} catch (UnsupportedEncodingException e) {
		//			e.printStackTrace();
		//		}


		if(owner==clientId){
			rep.write(obj.getDestFileName(), buf, pair.getValue());
		}else{
			SCPManager.remoteExternalRepWrite(obj.getDestFileName(), buf, pair.getValue(), diS.getRemotePeer(owner));
		}

		tempo = System.currentTimeMillis() - tempo;
		
		if(hash==null)
			Printer.println("  -> ExtRep: write " + obj.getDestFileName() + ", bOff = " + obj.getBlocksOffset().getValue() +", bId = " + obj.getBlocksOffset().getKey() + "NULL",  "verde");
		else{
			Printer.println("  -> ExtRep: write " + obj.getDestFileName() + ", bOff = " + obj.getBlocksOffset().getValue() +", bId = " + obj.getBlocksOffset().getKey() + " - OK!",  "verde");
		}
		
		return hash;
	}

	public void delete(String fileId, String hash){

	}

	public FileRepository getFileRepository() {
		return rep;
	}

	public void truncate(String path, long totalSize) {
		rep.truncate(path, totalSize);
	}

}
