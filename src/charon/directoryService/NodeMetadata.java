package charon.directoryService;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import charon.configuration.Location;
import charon.util.ExternalMetadataDummy;
import charon.util.IOUtil;
import depsky.client.messages.metadata.ExternalMetadata;
import fuse.FuseFtype;

public class NodeMetadata implements Cloneable, Externalizable {

	/**
	 * 
	 */
	//DEFAULT METADATA VALUES.
	public static final int MODE_DIR_DEFAULT = FuseFtype.TYPE_DIR | 0755; 
	public static final int MODE_FILE_DEFAULT = FuseFtype.TYPE_FILE | 0644;
	public static final int NLINK_DEFAULT = 1;
	public static final int UID_DEFAULT = Integer.parseInt(System.getProperty("uid")); ;
	public static final int GID_DEFAULT = Integer.parseInt(System.getProperty("gid")); ;
	public static final int RDEV_DEFAULT = 0;
	public static final int SIZE_DEFAULT = 0;
	public static final long BLOCKS_DEFAULT = 0;
	public static final int ATIME_DEFAULT = (int) (System.currentTimeMillis() / 1000L);
	public static final int MTIME_DEFAULT = (int) (System.currentTimeMillis() / 1000L);
	public static final int CTIME_DEFAULT = (int) (System.currentTimeMillis() / 1000L);
	private static byte[] EMPTY_HASH = new byte[0];



	private String nsPathId;

	// BasicMetadata
	private String parent;
	private String name;
	private String idpath;
	private int mode;
	private Map<Integer, ExternalMetadata> dataHash;
	private NodeType nodeType;

	//Stats
	private long inode;
	private int nlink;
	private int uid;
	private int gid; 
	private int rdev;
	private long size; 
	private long blocks;
	private long atime;
	private long mtime;
	private long ctime;

	private HashMap<String, byte[]> xttr;
	private boolean isPrivate;
	private boolean pending;
	private Location location;
	private String externalManaged;


	//TODO:
	//	private String linkToPath;

	public NodeMetadata() {
	}

	public NodeMetadata(String parent, String name, 
			long inode, int nlink, int uid, int gid, int rdev, long size, long blocks, int atime, int mtime, int ctime,  boolean isPending, boolean isPrivate,
			String idPath, int mode, byte[] hash, NodeType nodeType, Location location) {


		this.parent = parent;
		this.name = name;
		this.idpath = idPath;
		this.mode = mode;
		this.dataHash = new ConcurrentHashMap<Integer,ExternalMetadata>();
		this.nodeType = nodeType;

		this.inode = inode;
		this.mode = mode;
		this.nlink = nlink;
		this.uid = uid;
		this.gid = gid;
		this.rdev = rdev;
		this.size = size;
		this.blocks = blocks;
		this.atime = atime;
		this.mtime = mtime;
		this.ctime = ctime;
		this.pending = isPending;
		this.xttr = new HashMap<String, byte[]>();
		this.isPrivate = isPrivate;
		this.nsPathId = new String();
		this.location = location;
		

	}

	public NodeMetadata(String parent, String name, 
			long inode, int nlink, int uid, int gid, int rdev, long size, long blocks, int atime, int mtime, int ctime,  boolean isPending, boolean isPrivate,
			String idPath, int mode, byte[] hash, NodeType nodeType, Location location, String externalManaged) {

		this(parent, name, inode, nlink, uid, gid, rdev, size, blocks, atime, mtime, ctime, isPending, isPrivate, idPath, mode, hash, nodeType, location);
		this.externalManaged = externalManaged;

	}

	public Location getLocation() {
		return location;
	}

	public boolean isDirectory() {
		return nodeType.equals(NodeType.DIR);
	}

	public boolean isFile() {
		return nodeType.equals(NodeType.FILE);
	}


	public boolean isSymLink() {
		return nodeType.equals(NodeType.SYMLINK);
	}

	public void setId_path(String id_path) {
		this.idpath = id_path;
	}

	public boolean isPrivate(){
		return isPrivate;
	}

	public boolean setIsPrivate(boolean isPrivate){
		return this.isPrivate = isPrivate;
	}

	public String getNSPathId(){
		return this.nsPathId;
	}

	public void setNSPathId(String nsPathId){
		this.nsPathId = nsPathId;
	}

	public Map<String, byte[]> getXattr() {
		return xttr;
	}

	public long getInode() {
		return inode;
	}

	public void setInode(long inode) {
		this.inode = inode;
	}

	public void setMode(int mode) {
		this.mode = mode;
	}

	public int getNlink() {
		return nlink;
	}

	public void setNlink(int nlink) {
		this.nlink = nlink;
	}

	public boolean isPending(){
		return pending;
	}

	public void setPending(boolean isPending){
		this.pending = isPending;
	}

	public int getUid() {
		return uid;
	}

	public void setUid(int uid) {
		this.uid = uid;
	}

	public int getGid() {
		return gid;
	}

	public void setGid(int gid) {
		this.gid = gid;
	}

	public int getRdev() {
		return rdev;
	}

	public void setRdev(int rdev) {
		this.rdev = rdev;
	}

	public long getSize() {
		return size;
	}

	public void setSize(long size) {
		this.size = size;
	}

	public long getBlocks() {
		return blocks;
	}

	public void setBlocks(long blocks) {
		this.blocks = blocks;
	}

	public long getAtime() {
		return atime;
	}

	public void setAtime(long atime) {
		this.atime = atime;
	}

	public void setMtime(long mtime) {
		this.mtime = mtime;
	}

	public long getMtime() {
		return mtime;
	}

	public void setCtime(long ctime) {
		this.ctime = ctime;
	}

	public long getCtime() {
		return ctime;
	}

	public String getParent() {
		return parent;
	}

	public String getName() {
		return name;
	}

	public String getPath(){
		return genPath(parent, name);
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setParent(String parent) {
		this.parent = parent;
	}

	public String getIdpath() {
		return idpath;
	}
	public void setIdpath(String idpath) {
		this.idpath = idpath;
	}
	public int getMode() {
		return mode;
	}

	public Map<Integer, ExternalMetadata> getDataHashList(){
		return dataHash;
	}


	public void setDataHash(int block , ExternalMetadata hash){
		dataHash.put(block, hash);
	}

	public void setDataHashMap(Map<Integer,ExternalMetadata> hashs){
		dataHash = hashs;
	}


	public NodeType getNodeType() {
		return nodeType;
	}

	public void setNodeType(NodeType nodeType) {
		this.nodeType = nodeType;
	}

	@Override
	public String toString() {
		return "[ " + nodeType.getAsString() + ", " + getPath() + ", id=" + idpath + ", " + "size:"+ size + ", isPending:" + isPending() + " ]";
	}

	@Override
	public Object clone() throws CloneNotSupportedException {
		NodeMetadata m = new NodeMetadata();

		m.nsPathId = new String(this.nsPathId);

		// BasicMetadata
		m.parent = new String(this.parent);
		m.name = new String(this.name);
		m.idpath = new String(this.idpath);
		m.mode = this.mode;
		m.dataHash = this.dataHash;
		m.nodeType = this.nodeType;
		m.location  = this.location;


		//Stats
		m.inode = this.inode;
		m.nlink = this.nlink;
		m.uid = this.uid;
		m.gid = this.gid; 
		m.rdev = this.rdev;
		m.size = this.size; 
		m.blocks = this.blocks;
		m.atime = this.atime;
		m.mtime = this.mtime;
		m.ctime = this.ctime;

		m.xttr = this.xttr;
		m.isPrivate = this.isPrivate;
		m.pending = this.pending;

		return m;
	}


	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeUTF(nsPathId);
		writeExternalBasic(out);
		writeExternalStats(out);
		if(externalManaged == null){
			out.writeInt(-1);
		}else{
			out.writeInt(0);
			out.writeUTF(externalManaged);
		}

		out.flush();

	}

	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		nsPathId = in.readUTF();
		readExternalBasic(in);
		readExternalStats(in);

		if(in.readInt()!=-1)
			this.externalManaged = in.readUTF();
		else
			this.externalManaged = null;
	}

	private void writeExternalStats(ObjectOutput out) throws IOException {
		out.writeLong(inode); //inode;
		out.writeInt(nlink);//nlink;
		out.writeInt(uid);//uid;
		out.writeInt(gid);//gid; 
		out.writeInt(rdev);//rdev;
		out.writeLong(size);//size; 
		out.writeLong(blocks);//blocks;
		out.writeLong(atime);//atime;
		out.writeLong(mtime);//mtime;
		out.writeLong(ctime);//ctime;

		//xttr;
		writeExtrXttr(out);

		out.writeBoolean(isPrivate); //isPrivate;
		out.writeBoolean(pending); //pending;

		out.flush();

	}




	private void readExternalStats(ObjectInput in) throws IOException,
	ClassNotFoundException {
		inode = in.readLong(); //inode;
		nlink = in.readInt();//nlink;
		uid = in.readInt();//uid;
		gid = in.readInt();//gid; 
		rdev = in.readInt();//rdev;
		size = in.readLong();//size; 

		blocks = in.readLong();//blocks;
		atime = in.readLong();//atime;
		mtime = in.readLong();//mtime;
		ctime = in.readLong();//ctime;

		//xttr;
		readExtrXttr(in);

		isPrivate = in.readBoolean(); //isPrivate;
		pending = in.readBoolean(); //pending;


	}

	private void writeExtrXttr(ObjectOutput out) throws IOException{
		out.writeInt(xttr.size());
		for(Entry<String, byte[]> e : xttr.entrySet()){
			out.writeUTF(e.getKey());
			out.writeInt(e.getValue().length);
			for(int i = 0 ; i < e.getValue().length ; i++)
				out.write(e.getValue()[i]);
		}
	}

	private void readExtrXttr(ObjectInput in) throws IOException{
		xttr = new HashMap<String, byte[]>();
		int len = in.readInt();
		for(int i = 0 ; i<len;i++){
			String key = in.readUTF();
			int lenValue = in.readInt();
			byte[] value = new byte[lenValue];
			IOUtil.readFromOIS(in, value);
			xttr.put(key, value);
		}
	}

	private void readExternalBasic(ObjectInput in) throws IOException, ClassNotFoundException {
		parent = in.readUTF();
		name = in.readUTF();
		idpath = in.readUTF();
		location = Location.valueOf(in.readUTF());
		mode = in.readInt();

		//dataHash;
		int len = in.readInt();
		if(len==-1)
			dataHash=null;
		else{
			dataHash = new ConcurrentHashMap<Integer, ExternalMetadata>();
			for(int i = 0; i<len;i++){
				int key = in.readInt();

				ExternalMetadata versionInfo = null;

				//is ExternalMetadataDummy
				if(in.readBoolean())
					versionInfo = new ExternalMetadataDummy();
				else
					versionInfo = new ExternalMetadata();

				versionInfo.readExternal(in);
				dataHash.put(key, versionInfo);
			}
		}
		nodeType = NodeType.getNodeType(in.readInt()); //nodeType;
	}

	private void writeExternalBasic(ObjectOutput out) throws IOException {
		out.writeUTF(parent);
		out.writeUTF(name);
		out.writeUTF(idpath);
		out.writeUTF(location.name());
		out.writeInt(mode);

		//dataHash;
		if(dataHash==null){
			out.writeInt(-1);
			//			System.err.println("hash len = " + -1);
		}else{
			out.writeInt(dataHash.size());
			for(Entry<Integer, ExternalMetadata> block : dataHash.entrySet()){
				out.writeInt(block.getKey());
				out.writeBoolean(block.getValue() instanceof ExternalMetadataDummy);
				block.getValue().writeExternal(out);
			}
		}
		out.writeInt(nodeType.getAsInt()); //nodeType
		out.flush();
	}

	private String genPath(String parent, String name){
		if(parent.equals(""))
			return name;
		if(parent.equals("/"))
			return parent.concat(name);

		return parent.concat("/").concat(name);
	}

	public static NodeMetadata getDefaultNodeMetadata(String parent, String name, NodeType nodeType, long inode, String pathId, Location location){
		//		if(nodeType.equals(NodeType.DIR))
		//			return new DirectoryNodeMetadata(parent, name, 
		//					inode, NLINK_DEFAULT, UID_DEFAULT, GID_DEFAULT, RDEV_DEFAULT, 
		//					SIZE_DEFAULT, BLOCKS_DEFAULT, ATIME_DEFAULT, MTIME_DEFAULT, CTIME_DEFAULT, nodeType.equals(NodeType.DIR) ? false : true, true,
		//							pathId, nodeType.equals(NodeType.DIR) ? MODE_DIR_DEFAULT : MODE_FILE_DEFAULT, EMPTY_HASH, nodeType, Location.CoC);
		//		else
		return new NodeMetadata(parent, name, 
				inode, NLINK_DEFAULT, UID_DEFAULT, GID_DEFAULT, RDEV_DEFAULT, 
				SIZE_DEFAULT, BLOCKS_DEFAULT, ATIME_DEFAULT, MTIME_DEFAULT, CTIME_DEFAULT, nodeType.equals(NodeType.DIR) ? false : true, true,
						pathId, nodeType.equals(NodeType.DIR) ? MODE_DIR_DEFAULT : MODE_FILE_DEFAULT, EMPTY_HASH, nodeType, location);

	}

	public static NodeMetadata getDefaultNodeMetadata(String parent, String name, NodeType nodeType, long inode, String pathId, String externalManaged){
		//		if(nodeType.equals(NodeType.DIR))
		//			return new DirectoryNodeMetadata(parent, name, 
		//					inode, NLINK_DEFAULT, UID_DEFAULT, GID_DEFAULT, RDEV_DEFAULT, 
		//					SIZE_DEFAULT, BLOCKS_DEFAULT, ATIME_DEFAULT, MTIME_DEFAULT, CTIME_DEFAULT, nodeType.equals(NodeType.DIR) ? false : true, true,
		//							pathId, nodeType.equals(NodeType.DIR) ? MODE_DIR_DEFAULT : MODE_FILE_DEFAULT, EMPTY_HASH, nodeType, Location.CoC);
		//		else
		return new NodeMetadata(parent, name, 
				inode, NLINK_DEFAULT, UID_DEFAULT, GID_DEFAULT, RDEV_DEFAULT, 
				SIZE_DEFAULT, BLOCKS_DEFAULT, ATIME_DEFAULT, MTIME_DEFAULT, CTIME_DEFAULT, nodeType.equals(NodeType.DIR) ? false : true, true,
						pathId, nodeType.equals(NodeType.DIR) ? MODE_DIR_DEFAULT : MODE_FILE_DEFAULT, EMPTY_HASH, nodeType, Location.CoC, externalManaged);

	}

	public void setLocation(Location l) {
		location = l;		
	}

	public boolean isExternalManaged() {
		return externalManaged!=null;
	}

	public String getExternalManaged() {
		return externalManaged;
	}

}