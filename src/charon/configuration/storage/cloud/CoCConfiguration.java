package charon.configuration.storage.cloud;

import java.util.ArrayList;
import java.util.List;

import charon.configuration.Location;
import charon.configuration.storage.AStorageConfiguration;

public class CoCConfiguration extends AStorageConfiguration {

	private List<SingleCloudConfiguration> clouds;


	public CoCConfiguration(SingleCloudConfiguration... clouds) {
		this.clouds = new ArrayList<SingleCloudConfiguration>();
		for(SingleCloudConfiguration conf : clouds)
			this.clouds.add(conf);
		location = Location.CoC;
	}

	public CoCConfiguration(List<SingleCloudConfiguration> clouds) {
		this.clouds = clouds;
		location = Location.CoC;
	}

	public List<SingleCloudConfiguration> getCloudsConfiguration() {
		return clouds;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb = sb.append(location.toString()).append("{");
		for(SingleCloudConfiguration conf : clouds){
			sb = sb.append(conf.toString()).append("\n\n");
		}
		sb.setLength(sb.length()-1);
		sb=sb.append("}");
		return sb.toString();
	}
	
	public List<String[][]> getDepSkyFormatCredentials(){
		List<String[][]> res = new ArrayList<String[][]>(clouds.size());
		for(int i = 0 ; i<clouds.size() ; i++){
			res.add(clouds.get(i).getCloudAccessorFormatCredentials());
		}
		return res;
	}

}
