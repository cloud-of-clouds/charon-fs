package charon.configuration.storage.cloud;

import java.util.Properties;

import charon.configuration.Location;
import charon.configuration.storage.AStorageConfiguration;
import depsky.client.DepSkyClient;

public class SingleCloudConfiguration extends AStorageConfiguration{

	private String driverType;
	private String accessKey;
	private String secretKey;
	private String cloudLocation;
	private Properties others;
	private String driverId;
	
	public SingleCloudConfiguration(String driverType, String driverId, String accessKey, String secretKey, String cloudLocation) {
		this.driverType = driverType;
		this.driverId = driverId;
		this.accessKey = accessKey;
		this.secretKey = secretKey;
		this.cloudLocation = cloudLocation;
		this.location = Location.SINGLE_CLOUD;
                if(accessKey.equals("") || secretKey.contains("")){
                    getEnvCredentials();
                }
        }
	
	public String getDriverType() {
		return driverType;
	}

	public String getAccessKey() {
		return accessKey;
	}

	public String getSecretKey() {
		return secretKey;
	}

	public String getCloudLocation() {
		return cloudLocation;
	}

	public Properties getOthers() {
		return others;
	}

	public String getDriverId() {
		return driverId;
	}

	public void setOtherParams(Properties props){
		this.others = props; 
	}
	
	public Properties getOtherParams() {
		return others;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb = sb.append(location.toString()).append("{");
		sb = sb.append("driver.type=").append(driverType).append("\n");
		sb = sb.append("driver.id=").append(driverId != null ? driverId : "").append("\n");
		sb = sb.append("accessKey=").append(accessKey).append("\n");
		sb = sb.append("secretKey=").append(secretKey).append("\n");
		sb = sb.append("location=").append(cloudLocation != null ? cloudLocation : "");
		sb = sb.append("}");
		
		return sb.toString();
	}

	public String[][] getCloudAccessorFormatCredentials() {
		String[][] res = new String[4][2];
		res [0] = new String[] {"driver.type",driverType};
		res [1] = new String[] {"driver.id",driverId};
		res [2] = new String[] {"accessKey",accessKey};
		res [3] = new String[] {"secretKey",secretKey};
		return res;
	}

    private void getEnvCredentials() {
        if(driverType.equals("AMAZON-S3")){
            String str = System.getenv().get("AWS_ACCESS_KEY_ID");
            if(str!=null)
                accessKey = str;
            
            str = System.getenv().get("AWS_SECRET_ACCESS_KEY");
            if(str!=null)
                secretKey = str;
            
        }
    }
	
	
	
}
