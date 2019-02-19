package charon.configuration.storage;

import charon.configuration.Location;
import charon.configuration.storage.cloud.CoCConfiguration;
import charon.configuration.storage.cloud.SingleCloudConfiguration;
import charon.configuration.storage.remote.RemoteConfiguration;


public interface IStorageConfiguration {

	public Location getLocation();
	
	public CoCConfiguration getAsCoCConfig();
	
	public PrivateRepConfiguration getAsLocalConfig();
	
	public RemoteConfiguration getAsRemoteConfig();
	
	public SingleCloudConfiguration getAsSingleCloudConfig();
	
}
