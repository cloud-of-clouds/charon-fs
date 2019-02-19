package charon.configuration.storage;

import charon.configuration.Location;

public class ExternalRepConfiguration extends AStorageConfiguration {
	
	private String path;

	public ExternalRepConfiguration(String path) {
		this.path = path;
		this.location = Location.EXTERNAL_REP;
	}
	
	@Override
	public String toString() {
		return location.toString() + "{" + path + "}";
	}

	public String getPath() {
		return this.path;
	}
	
}
