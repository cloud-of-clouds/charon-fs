package charon.directoryService;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import charon.general.Printer;
import charon.storageService.accessors.DepSkyAcessor;
import charon.util.IOUtil;
import depsky.client.messages.metadata.ExternalMetadata;

public class FileVersionsManager {

	private final int DELTA = 5000;
	private String idToLookUp;
	private DepSkyAcessor acessor;
	private Map<String, FileHashRepresentation> recentVersions;
	private long lastRead;

	public FileVersionsManager(String idToLookUp, DepSkyAcessor accessor){


		this.idToLookUp = idToLookUp.concat("#").concat(idToLookUp);
		this.recentVersions = new HashMap<String, FileHashRepresentation>();
		this.acessor = accessor;
		//		readVersions();
		lastRead = System.currentTimeMillis();
	}


	private void readVersions() {
				System.out.println("--- version file read: ");
		byte[] array = acessor.readFromDepSky(idToLookUp+"versions");
		

		if(array!=null){
			ByteArrayInputStream bais = new ByteArrayInputStream(array);
			try {
			ObjectInputStream ois = new ObjectInputStream(bais);
				int size = ois.readInt();
				if(size > -1){
					LinkedList<FileHashRepresentation> list = new LinkedList<FileHashRepresentation>(); 
					FileHashRepresentation aux = null;
					for(int i = 0 ; i<size ; i++){
						aux = new FileHashRepresentation();
						aux.readExternal(ois);
						list.add(aux);
					}
					for(FileHashRepresentation filehash : list){
						recentVersions.put(filehash.getPathId(), filehash);
					}
				}

				IOUtil.closeStream(bais);
				IOUtil.closeStream(ois);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				System.out.println(e.getLocalizedMessage());
			}
		}else
			System.out.println("versionFileRead é NULLLLLL");


	}

	public FileHashRepresentation getFileHashRep(String pathId){
		if(recentVersions.get(pathId) == null || System.currentTimeMillis() - lastRead > DELTA ){
			//			System.out.println("FileVersion: Fui ler.");
			readVersions();
			lastRead = System.currentTimeMillis();
		}
		return recentVersions.get(pathId);
	}

	public Map<Integer, ExternalMetadata> getHashs(String idPath){
		FileHashRepresentation hashRep = getFileHashRep(idPath);
		if( hashRep != null )
			return recentVersions.get(idPath).getHashes();
		else{
			Printer.println("--- GetHashFile:74 - null", "vermelho");
			return null;
		}
	}

	public long getDataSize(String idPath){
		FileHashRepresentation hashRep = getFileHashRep(idPath);
		if( hashRep != null )
			return recentVersions.get(idPath).getFileSize();
		else{
			Printer.println("--- GetHashFile:84 - null", "vermelho");
			return -1;
		}
	}

}
