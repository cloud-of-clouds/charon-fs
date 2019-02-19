package charon.storageService.accessors;

import java.io.File;

import charon.configuration.CharonConfiguration;
import charon.directoryService.DirectoryServiceImpl;
import charon.general.NSAccessInfo;
import charon.general.Printer;
import charon.storageService.DiskCacheManager;
import charon.storageService.repositories.FileRepository;
import charon.storageService.repositories.FileRepositoryFactory;
import charon.storageService.scpSockets.SCPManager;
import charon.util.compress.DeflaterCompressionUtil;
import depsky.util.integrity.IntegrityManager;

public class PrivateRepositoryAcessor {

	private FileRepository localRep;
	private DiskCacheManager disk;
	private boolean useCompression;
	private DirectoryServiceImpl diS;
	private int clientId;


	public PrivateRepositoryAcessor(CharonConfiguration config, DirectoryServiceImpl diS){
		this.diS = diS;
		this.disk = new DiskCacheManager(config.getCacheDirectory());
		this.localRep = FileRepositoryFactory.build(config.getPrivateRepositoryDirectory(), config.getCacheDirectory()); 
		this.useCompression = config.useCompression();
		// ==============

		this.clientId = config.getClientId();

	}

	public byte[] read(String fileId, String hash){
		String[] names = fileId.split("#", 2);
		NSAccessInfo snsInfo = diS.getSNSAccessInfo(names[0]);
		if(snsInfo == null)
			return null;

		int owner = snsInfo.getOwnerId();
		byte[] data = null;


		long tempo = System.currentTimeMillis();
		if(owner == clientId){
			data = localRep.read(fileId.replace("#", File.separator) + "-" + hash);
		}else{
			data = SCPManager.remoteLocalRepRead(fileId, hash, diS.getRemotePeer(owner));
		}

		tempo = System.currentTimeMillis() - tempo;


		if(data!=null  && useCompression)
			data = DeflaterCompressionUtil.decompress(data);

		if(data==null)
			Printer.println("  -> LocalRep: read " +fileId+" - NULL response. [took: " + Long.toString(tempo) + " ms].", "verde");
		else{
			Printer.println("  -> LocalRep: read " +fileId+" - OK! [took: " + Long.toString(tempo) + " ms].", "verde");
		}

		return data;

	}

	public String write(String fileId){
		String[] names = fileId.split("#", 2);

		NSAccessInfo snsInfo = diS.getSNSAccessInfo(names[0]);
		if(snsInfo == null)
			return null;

		int owner = snsInfo.getOwnerId();
		byte[] buf = null;
		buf = disk.readWhole(fileId);
		if(buf == null)
			return null;

		if(useCompression)
			buf = DeflaterCompressionUtil.compress(buf);

		String hash = IntegrityManager.getHexHash(buf);
		long tempo = System.currentTimeMillis();


		if(owner==clientId){
			localRep.write(fileId.replace("#", File.separator) + "-" + hash, buf);
		}else{
			SCPManager.remoteLocalRepWrite(fileId, buf, hash, diS.getRemotePeer(owner));
		}
		
		tempo = System.currentTimeMillis() - tempo;

		if(hash==null)
			Printer.println("  -> LocalRep: write " + fileId +" - NULL response. [took: " + Long.toString(tempo) + " ms].", "verde");
		else{
			Printer.println("  -> LocalRep: write " + fileId +" - OK! [took: " + Long.toString(tempo) + " ms].", "verde");
		}

		return hash;
	}

	public void delete(String fileId, String hash){
		
	}

	public FileRepository getFileRepository() {
		return localRep;
	}

}
