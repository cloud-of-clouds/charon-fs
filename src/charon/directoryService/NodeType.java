package charon.directoryService;

public enum NodeType {
	FILE, DIR, SYMLINK, PRIVATE_NAMESPACE;
	
	public String getAsString() {
		return this.name();
	}
	
	public static NodeType getNodeType(String name){
		if(name.equalsIgnoreCase("FILE"))
			return FILE;
		if(name.equalsIgnoreCase("DIR"))
			return DIR;
		if(name.equalsIgnoreCase("SYMLINK"))
			return SYMLINK;
		if(name.equalsIgnoreCase("PRIVATE_NAMESPACE"))
			return PRIVATE_NAMESPACE;
		return null;
	}
	
	public int getAsInt(){
		return this.ordinal();
	}
	
	public static NodeType getNodeType(int ordinal){
		if(ordinal == NodeType.FILE.ordinal())
			return FILE;
		if(ordinal == NodeType.DIR.ordinal())
			return DIR;
		if(ordinal == NodeType.SYMLINK.ordinal())
			return SYMLINK;
		if(ordinal == NodeType.PRIVATE_NAMESPACE.ordinal())
			return PRIVATE_NAMESPACE;
		return null;
	}
	
}
