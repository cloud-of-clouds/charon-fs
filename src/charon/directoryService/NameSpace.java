package charon.directoryService;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import charon.configuration.storage.remote.RemoteConfiguration;
import charon.directoryService.exceptions.DirectoryServiceException;
import charon.general.NSAccessInfo;
import depsky.client.messages.metadata.ExternalMetadata;
import fuse.FuseDirFiller;

public class NameSpace implements Externalizable{

	private String id;
	private NSAccessInfo snsInfo;
	private ConcurrentHashMap<String, NodeMetadata> metadataBag;
	private ConcurrentHashMap<String, String> idPathToPath;
	private long version;
	private RemoteConfiguration remoteConfiguration;

	public NameSpace() {
		this.metadataBag = new ConcurrentHashMap<String, NodeMetadata>();
	}

	public NameSpace(String id, int[] owners, NSAccessInfo snsInfo) {
		this.id = id;
		this.metadataBag = new ConcurrentHashMap<String, NodeMetadata>();
		this.version = 0;
		this.snsInfo = snsInfo;
		this.idPathToPath = new ConcurrentHashMap<String, String>();
	}
	
	public void setRemoteConfiguration(RemoteConfiguration remoteConfiguration) {
		this.remoteConfiguration = remoteConfiguration;
	}

	public RemoteConfiguration getRemoteConfiguration() {
		return remoteConfiguration;
	}
	
	public String getId() {
		return id;
	}

	public Collection<NodeMetadata> getAllNodes(){
		return metadataBag.values();
	}

	//	public void addOwners(int[] own ){
	//		if(own == null)
	//			return;
	//		for(int i = 0 ; i<owners.length ; i++)
	//			for(int j = 0 ; j<own.length ; j++){
	//				if(owners[i]==own[j])
	//					own[j]=-1;
	//			}
	//
	//		int[] n_owners = new int[owners.length + own.length];
	//		int index = 0;
	//		for(int i=0 ; i<owners.length ; i++){
	//			n_owners[index] = owners[i];
	//			index++;
	//		}
	//		for(int i = 0 ; i<own.length; i++){
	//			if(own[i]!=-1)
	//				n_owners[index] = own[i];
	//			index++;
	//		}
	//		owners=n_owners;
	//	}

	public NSAccessInfo getSnsInfo() {
		return snsInfo;
	}

	public void setSnsInfo(NSAccessInfo snsInfo) {
		this.snsInfo = snsInfo;
	}

	public long getVersion() {
		return version;
	}

	public void putMetadata(NodeMetadata metadata) {
		if(metadata.getParent().equals("root")){
			String path = "/";
			metadataBag.put(path, metadata);
			this.idPathToPath.put(metadata.getIdpath(), path);
		}else{
			this.metadataBag.put(metadata.getPath(), metadata);
			this.idPathToPath.put(metadata.getIdpath(), metadata.getPath());
		}
	}

	public void updateMetadata(String path, NodeMetadata metadata)  {

		this.metadataBag.put(metadata.getPath(), metadata);

		if(!path.equals(metadata.getPath())){
			this.metadataBag.remove(path);
			this.idPathToPath.put(metadata.getIdpath(), metadata.getPath());

			for(NodeMetadata mdata : metadataBag.values()){
//				if(mdata instanceof DirectoryNodeMetadata){
//					DirectoryNodeMetadata dirMdata = (DirectoryNodeMetadata) mdata;
//					if(dirMdata.getPath().equals(metadata.getParent()))
//						dirMdata.putChild(metadata);
//					else
//						dirMdata.removeChild(path);
//				}

				if(mdata.getParent().equals(path)){
					String p = mdata.getPath();
					mdata.setParent(metadata.getPath());
					updateMetadata(p, mdata);
				}

			}
		}
	}

	public void insertMetadataInBuffer(String path, NodeMetadata metadata) throws DirectoryServiceException {
		metadataBag.put(path, metadata);

	}

	public void commitMetadataBuffer(String idPath, ExternalMetadata hash, int block) throws DirectoryServiceException {
		String path = getPathFromIdPath(idPath);
		if(path==null){
			System.out.println("NameSpace:commitMetadataBuffer: the NODEMETADATA is NULL! not suppose to.");
			return;
		}

		metadataBag.get(path).setDataHash(block, hash);
	}

	public NodeMetadata removeMetadata(String path) {

		return metadataBag.remove(path);

		//		if(metadata!=null && metadata.getIdpath().equals(this.id+"#"+"ROOT")){
		//			Printer.println(metadataBag.get(path).getIdpath() + ", " + this.id+"#"+"ROOT", "azul");
		//			metadataBag = new ConcurrentHashMap<String, NodeMetadata>();
		//			Printer.println("NS: number of nodes : " + getNumberOfNodes(), "azul"); 
		//			return true;
		//		}

		//		Printer.println("NS: number of nodes : " + getNumberOfNodes(), "azul"); 
	}

	public int getNumberOfNodes(){
		return metadataBag.size();
	}

	public NodeMetadata getMetadata(String path) {
		return metadataBag.get(path);
	}

	public String getPathFromIdPath(String idPath) {
		return idPathToPath.get(idPath);

		//		for(NodeMetadata meta : metadataBag.values()){
		//			if(meta.getIdpath().equals(idPath)){
		//				return meta.getPath();
		//			}
		//		}
		//		return null;
	}

	public NodeMetadata getMetadataFromIdPath(String idPath) {
		String path = idPathToPath.get(idPath);
		if(path == null)
			return null;

		return metadataBag.get(path);

		//		for(NodeMetadata meta : metadataBag.values()){
		//			if(meta.getIdpath().equals(idPath)){
		//				return meta;
		//			}
		//		}
		//		return null;
	}

	public boolean containsMetadata(String path){
		return metadataBag.containsKey(path);
	}

	public boolean containsIdPath(String idPath){
		return getPathFromIdPath(idPath) != null;
	}

	public void getNodeChildren(String path, FuseDirFiller dirFiller) {
		for(Entry<String, NodeMetadata> e : metadataBag.entrySet()){
			if(e.getValue().getParent().equals(path)){
				NodeMetadata m = e.getValue();
				dirFiller.add(m.getName(), m.getInode(), (int) m.getMode());
			}
		}
	}

	public boolean isFolderEmpty(String path) {
		for(Entry<String, NodeMetadata> e : metadataBag.entrySet())
			if(e.getValue().getParent().equals(path))
				return false;
		return true;
	}

	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

		//	long version;
		//	String id;
		//	int[] owners;
		//	ConcurrentHashMap<String, NodeMetadata> metadataBag;
		idPathToPath = new ConcurrentHashMap<String, String>();
		version = in.readLong();
		//		System.out.println("version = " +version );

		id = in.readUTF();
		//		owners = new int[in.readInt()];

		//		for(int i = 0 ; i<owners.length ; i++){
		//			owners[i] = in.readInt();
		//		}

		int len = in.readInt();
		for(int i = 0 ; i<len ; i++){
			String key = in.readUTF();
			NodeMetadata nm = null;
//			if(in.readBoolean())
//				nm = new DirectoryNodeMetadata();
//			else
				nm = new NodeMetadata();
			nm.readExternal(in);
			metadataBag.put(key, nm);
			idPathToPath.put(nm.getIdpath(), nm.getPath());
		}

		snsInfo = (NSAccessInfo)in.readObject();
		
		len = in.readInt();
		if(len!=-1){
			this.remoteConfiguration=new RemoteConfiguration();
			remoteConfiguration.readExternal(in);
		}
			
	}

	public void writeExternal(ObjectOutput out) throws IOException {
		//	long version;
		//	String id;
		//	int[] owners;
		//	ConcurrentHashMap<String, NodeMetadata> metadataBag;

		out.writeLong(version);
		out.writeUTF(id);
		//		out.writeInt(owners.length);
		//		for(int i = 0 ; i<owners.length ; i++){
		//			out.writeInt(owners[i]);
		//		}

		Set<Entry<String, NodeMetadata>> l = metadataBag.entrySet();
		out.writeInt(l.size());
		for(Entry<String, NodeMetadata> e : l){
			out.writeUTF(e.getKey());
//			out.writeBoolean(e.getValue() instanceof DirectoryNodeMetadata);
			e.getValue().writeExternal(out);
		}

		out.writeObject(snsInfo);
		
		if(remoteConfiguration==null){
			out.writeInt(-1);
		}else{
			out.writeInt(0);
			remoteConfiguration.writeExternal(out);
		}
		
		out.flush();
	}

	public void writeMetadataHashs(ObjectOutputStream oos) {
		List<FileHashRepresentation> list = new LinkedList<FileHashRepresentation>();
		for(NodeMetadata nm : metadataBag.values()){
			if(nm.isFile()){
				list.add(new FileHashRepresentation(nm.getIdpath(), nm.getSize(), nm.getDataHashList()));
			}
		}
		try {
			oos.writeInt(list.size());
			for(FileHashRepresentation fr : list)
				fr.writeExternal(oos);
			oos.flush();
		} catch (IOException e) {
		}
	}

	public void incVersion() {
		version++;
	}

	public Collection<? extends NodeMetadata> getAllLinks(String idPath) {
		return new ArrayList<NodeMetadata>();
	}

	public void getNodeChildren(String path, Set<String> res) {
		for(Entry<String, NodeMetadata> e : metadataBag.entrySet()){
			if(e.getValue().getParent().equals(path)){
				NodeMetadata m = e.getValue();
				res.add(m.getPath());
			}
		}
	}

//	public boolean putChildren(NodeMetadata m) {
//		DirectoryNodeMetadata parent = null;
//		NodeMetadata parentNode = getMetadata(m.getParent());
//		if(parentNode != null && parentNode instanceof DirectoryNodeMetadata)
//			parent = (DirectoryNodeMetadata) parentNode;
//
//		if(parent==null)
//			return false;
//		parent.putChild(m);
//		return true;
//	}

//	public boolean removeChild(String parentPath, String path) {
//		DirectoryNodeMetadata parent = (DirectoryNodeMetadata) getMetadata(parentPath);
//		if(parent==null)
//			return false;
//		parent.removeChild(path);
//		return true;
//	}




}
