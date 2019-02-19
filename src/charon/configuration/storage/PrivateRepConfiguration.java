package charon.configuration.storage;

import charon.configuration.Location;


public class PrivateRepConfiguration extends AStorageConfiguration {
	
	private String path;
	
	public PrivateRepConfiguration(String path) {
		this.path = path;
		this.location = Location.PRIVATE_REP;
	}
	
	@Override
	public String toString() {
		return location.toString() + "{" + path + "}";
	}

	public String getPath() {
		return path;
	}

	
}
