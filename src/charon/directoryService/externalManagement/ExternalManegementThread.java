package charon.directoryService.externalManagement;

import java.util.ArrayList;
import java.util.Set;

import charon.configuration.Location;
import charon.directoryService.DirectoryServiceImpl;
import charon.directoryService.NameSpace;
import charon.directoryService.NodeMetadata;
import charon.directoryService.NodeType;
import charon.directoryService.exceptions.DirectoryServiceException;
import charon.general.Charon;
import charon.storageService.StorageService;
import charon.util.ExternalMetadataDummy;
import depsky.util.Pair;

public class ExternalManegementThread extends Thread {

	private int period;
	private ArrayList<Pair<ExternalFile, NodeMetadata>> foldersToWatch;
	private DirectoryServiceImpl diS;

	private boolean finish; 

	public ExternalManegementThread(int period, DirectoryServiceImpl diS) {
		this.period = period;
		this.diS = diS;
		this.finish = false;
		this.foldersToWatch = new ArrayList<Pair<ExternalFile, NodeMetadata>>();
	}

	@Override
	public void run() {

		while(!finish){
			synchronized (foldersToWatch) {
				for(Pair<ExternalFile, NodeMetadata> pair : foldersToWatch){
					addFilesToDirectoryService(pair.getKey(), pair.getValue());
				}
			}
			//sleep for the period
			try { Thread.sleep(period); } catch (InterruptedException e) {}
		}
	}

	private void addFilesToDirectoryService(ExternalFile folder, NodeMetadata parentDirectory){
		NameSpace ns = diS.getNS(parentDirectory.getPath());
		if(ns == null)
			return;

		if(!folder.exists()){
			if(folder.isDirectory()){
				Set<String> set = diS.getNodeChildren(parentDirectory.getPath()+"/"+folder.getName());
				for(String s: set){
					diS.removeMetadata(s, ns);
				}
			}
			diS.removeMetadata(parentDirectory.getPath()+"/"+folder.getName(), ns);
			return;
		}


		if(folder.isFile()){
			//				BasicFileAttributes attr = Files.readAttributes(folder.toPath(), BasicFileAttributes.class);
			NodeMetadata metadata = NodeMetadata.getDefaultNodeMetadata(parentDirectory.getPath(), folder.getName(), NodeType.FILE, System.currentTimeMillis(), Charon.getNextIdPath(ns), folder.getPathIdentifier());
			metadata.setPending(false);
			metadata.setSize(folder.size());
			metadata.setMtime(folder.lastModifiedTime());
			metadata.setAtime(folder.lastAccessTime());
			metadata.setCtime(folder.creationTime());
			metadata.setLocation(Location.EXTERNAL_REP);

			int numBlocks = StorageService.getBlockNumber(folder.size()-1);
			for(int i = 0 ; i<numBlocks ; i++){
				metadata.setDataHash(i, new ExternalMetadataDummy(""+System.nanoTime()));
			}

			NodeMetadata original;
			try {
				original = diS.getMetadata(metadata.getPath());
			} catch (DirectoryServiceException e) {
				diS.putMetadata(metadata, ns);
				return;
			}

			if(metadata.getSize() != original.getSize() || metadata.getMtime() != original.getMtime() || metadata.getCtime() != original.getCtime() ||metadata.getAtime() != original.getAtime() )
				diS.putMetadata(metadata, ns);
		}else if(folder.isDirectory()){
			NodeMetadata metadata = NodeMetadata.getDefaultNodeMetadata(parentDirectory.getPath(), folder.getName(), NodeType.DIR, System.currentTimeMillis(), Charon.getNextIdPath(ns), folder.getPathIdentifier());
			try {
				//see if it exists?
				metadata = diS.getMetadata(metadata.getPath());
			} catch (DirectoryServiceException e) {
				//if not, a exception is thrown, and insert it
				diS.putMetadata(metadata, ns);
			}

			Set<String> inCharonChildren = diS.getNodeChildren(metadata.getPath());

			ExternalFile[] children = folder.listFiles();
			for(ExternalFile child : children ){
				inCharonChildren.remove(metadata.getPath()+"/"+child.getName());
				addFilesToDirectoryService(child, metadata);
			}
			ns = diS.getNS(metadata.getPath());
			for(String s: inCharonChildren){
				diS.removeMetadata(s, ns);
			}

		}else{
			// TODO: what about symlinks and hardLinks???!?!?!?
		}
	}


	public void addFolderToWatch(String extPath, NodeMetadata internalPath) {
		synchronized (foldersToWatch) {
			ExternalFile extFile = ExternalFileFactory.build(extPath);
			if(extFile != null)
				foldersToWatch.add(new Pair<ExternalFile, NodeMetadata> (extFile, internalPath));
			else
				System.out.println("ERROR: No HDFS configuration dir founded! External File no added!");
		}
	}

	public boolean containsFolder(String externalFolder) {
		synchronized (foldersToWatch) {
			for(Pair<ExternalFile, NodeMetadata> f : foldersToWatch)
				if(f.getKey().getPathIdentifier().equals(externalFolder))
					return true;

		}
		return false;
	}

	public void remove(String folder) {
		synchronized (foldersToWatch) {
			int index = -1;
			for(int i = 0 ; i< foldersToWatch.size() ; i++){
				if(foldersToWatch.get(i).getKey().getPathIdentifier().equals(folder)){
					index = i;
					break;
				}
			}
			if(index!=-1)
				foldersToWatch.remove(index);
		}
	}

}
