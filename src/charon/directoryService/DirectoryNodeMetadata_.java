package charon.directoryService;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.ArrayList;
import java.util.List;

import charon.configuration.Location;

public class DirectoryNodeMetadata_ extends NodeMetadata {

	private List<NodeMetadata> children;

	public DirectoryNodeMetadata_() {	}

	public DirectoryNodeMetadata_(String parent, String name, 
			long inode, int nlink, int uid, int gid, int rdev, long size, long blocks, int atime, int mtime, int ctime,  boolean isPending, boolean isPrivate,
			String idPath, int mode, byte[] hash, NodeType nodeType, Location location) {
		super(parent, name, inode, nlink, uid, gid, rdev, size, blocks, atime, mtime, ctime, isPending, isPrivate, idPath, mode, hash, nodeType, location);
		this.children = new ArrayList<NodeMetadata>();
	}


	public void putChild(NodeMetadata m){
		children.add(m);
	}

	public NodeMetadata removeChild(String path){
		for(int i = 0 ; i<children.size() ; i++){
			NodeMetadata meta = children.get(i);
			if(meta.getPath().equals(path) || !meta.getParent().equals(getPath())){
				return children.remove(i);
			}
		}
		return null;
	}

	@Override
	public boolean isDirectory() {
		return true;
	}
	
	public void swapChildren(NodeMetadata m){
		for(int i = 0; i < children.size() ; i++){
			NodeMetadata current = children.get(i);
			if(current.getPath().equals(m.getPath())){
				if(current instanceof DirectoryNodeMetadata_){
					DirectoryNodeMetadata_ currentDir = (DirectoryNodeMetadata_) current;
					DirectoryNodeMetadata_ mDir = (DirectoryNodeMetadata_) m;
					
					for(NodeMetadata currChild : currentDir.getChildren() )
						mDir.swapChildren(currChild);
				}
				children.remove(i);
			}
		}
		children.add(m);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
	ClassNotFoundException {
		super.readExternal(in);
		children = new ArrayList<NodeMetadata>();
	}

	public List<NodeMetadata> getChildren() {
		return children;
	}

}
