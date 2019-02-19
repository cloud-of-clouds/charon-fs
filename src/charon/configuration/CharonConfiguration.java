package charon.configuration;

import charon.configuration.storage.AStorageConfiguration;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Scanner;

import charon.configuration.storage.ExternalRepConfiguration;
import charon.configuration.storage.PrivateRepConfiguration;
import charon.configuration.storage.cloud.CoCConfiguration;
import charon.configuration.storage.cloud.SingleCloudConfiguration;
import charon.general.CharonConstants;
import java.util.StringTokenizer;

public class CharonConfiguration {

    //DEFAULT FLAGS VALUES
    private final boolean DEFAULT_USE_MAIN_MEMORY_CACHE = false;
    private final boolean DEFAULT_DEBUG = false;
    private final boolean DEFAULT_FSYNC_TO_CLOUDS = false;
    private final boolean DEFAULT_PREFETCHING = true;
    private final boolean DEFAULT_TIMES = false;
    private final boolean DEFAULT_COMPRESSION = true;

    private final String TAG_TIMES = "times";

    //threads default values
    private final int DEFAULT_NUMBER_OF_SENDING_THREADS = 4;
    private final int DEFAULT_NUMBER_DEPSKY_DATA_THREADS = 16;
    private final int DEFAULT_NUMBER_DEPSKY_METADATA_THREADS = 4;
    private final int DEFAULT_NUMBER_OF_PREFETCHINGG_THREADS = 4;

    // Properties tags
    private final String TAG_CACHE_DIRECTORY = "cache.directory";
    private final String TAG_LOCAL_REP_SERVER_ADDR = "addr";
    private final String TAG_COMPRESSION = "compression";

    //threads tags
    private final String TAG_NUMBER_OF_SENDING_THREADS = "num.backgroud.threads";
    private final String TAG_NUMBER_DEPSKY_DATA_THREADS = "num.depsky.data.threads";
    private final String TAG_NUMBER_DEPSKY_METADATA_THREADS = "um.depsky.metadata.threads";
    private final String TAG_NUMBER_OF_PREFETCHINGG_THREADS = "num.prefetching.threads";
    private final String TAG_SHARE_TOKENS_FOLDER = "share.tokens.directory";
    private final String TAG_SITE_IDS_FOLDER = "site.ids.directory";

    private final String DEFAULT_SHARE_TOKENS_FOLDER = CharonConstants.SNS_FOLDER;
    private final String DEFAULT_SITE_IDS_FOLDER = CharonConstants.SITE_IDS_FOLDER;

    private final String TAG_USE_MAIN_MEMORY_CACHE = "main.memory.cache";
    private final String TAG_DEBUG = "debug.mode";
    private final String TAG_FSYNC_TO_CLOUDS = "fsync.to.cloud";
    private final String DEFAULT_LOCATION_TAG = "DEFAULT_LOCATION";

    private final String TAG_MOUNT_POINT = "mount.point";
    private final String TAG_CLIENT_ID = "client.id";
    private final String TAG_CLIENT_NAME = "client.name";

    private final String TAG_EMAIL = "email";

    private final String TAG_PNS_ID = "personal.namespace.id";

    private final String TAG_PREFETCHING = "prefetching";

    // Class attributes
    private CoCConfiguration cocConfig;
    private SingleCloudConfiguration singleCloudConfig;
    private PrivateRepConfiguration privateRepConfig;
    private ExternalRepConfiguration externalRepConfig;
    private Map<String, Location> cuesLocation;

    private String shareTokensDir;
    private String siteIdsDir;

    //	private String localRepositoryDirectory;
    //	private String remoteRepositoryDirectory;
    private String cacheDirectory;

    private boolean useMainMemoryCache;
    private boolean debug;
    private boolean fsyncToCloud;
    private boolean compression;

    private boolean times;

    private String mountPoint;
    private int clientId;
    private String clientName;

    private String email;

    private String localRepositoryServerAddress;

    private String pnsId;

    private boolean prefecting;

    //threads
    private int numOfSendingThreads;
    private int numOfPrefetchingThreads;
    private int numOfDepSkyDataThreads;
    private int numOfDepSkyMetadataThreads;
    private Location defaultLocation;

    public CharonConfiguration(int clientId, String mountPoint) {
        this.clientId = clientId;
        this.mountPoint = mountPoint;
        this.useMainMemoryCache = DEFAULT_USE_MAIN_MEMORY_CACHE;
        this.debug = DEFAULT_DEBUG;
        this.fsyncToCloud = DEFAULT_FSYNC_TO_CLOUDS;
        this.prefecting = DEFAULT_PREFETCHING;
        this.numOfSendingThreads = DEFAULT_NUMBER_OF_SENDING_THREADS;
        this.numOfPrefetchingThreads = DEFAULT_NUMBER_OF_PREFETCHINGG_THREADS;
        this.numOfDepSkyDataThreads = DEFAULT_NUMBER_DEPSKY_DATA_THREADS;
        this.numOfDepSkyMetadataThreads = DEFAULT_NUMBER_DEPSKY_METADATA_THREADS;
        this.times = DEFAULT_TIMES;
        this.compression = DEFAULT_COMPRESSION;
        this.shareTokensDir = DEFAULT_SHARE_TOKENS_FOLDER;
        this.siteIdsDir = DEFAULT_SITE_IDS_FOLDER;
    }

    public CharonConfiguration(Properties props, String locationsConfigFileName, String charonConfigFileName) throws ParseException {
        try {
            readStorageLocationsConfiguration(locationsConfigFileName, props);
        } catch (FileNotFoundException e1) {
            throw new ParseException("Error while trying to mount the system. [ File not found : " + locationsConfigFileName + " ]", 0);
        }

        String temp = props.getProperty(TAG_CLIENT_ID);
        if (temp != null && !temp.equals("")) {
            temp = solveVariables(props, CharonConstants.CHARON_CONFIG_FILE_NAME, temp);
        } else {
            throw new ParseException(charonConfigFileName + " : The client id value must be specified: (" + TAG_CLIENT_ID + "=[id])", 0);
        }

        try {
            this.clientId = Integer.parseInt(temp);
        } catch (NumberFormatException e) {
            throw new ParseException(charonConfigFileName + " : The client id value must be an integer.)", 0);
        }

        //USE COMPRESSION
        temp = props.getProperty(TAG_COMPRESSION);
        if (temp != null && !temp.equals("")) {
            temp = solveVariables(props, CharonConstants.CHARON_CONFIG_FILE_NAME, temp);
            if (temp.equalsIgnoreCase("true") || temp.equalsIgnoreCase("false")) {
                this.compression = temp.equalsIgnoreCase("true");
            } else {
                throw new ParseException(charonConfigFileName + " : The " + TAG_COMPRESSION + " value must be a boolean.)", 0);
            }
        } else {
            this.compression = DEFAULT_COMPRESSION;
        }

        // THREAD NUMBER //
        temp = props.getProperty(TAG_NUMBER_OF_SENDING_THREADS);
        if (temp != null && !temp.equals("")) {
            temp = solveVariables(props, CharonConstants.CHARON_CONFIG_FILE_NAME, temp);
            try {
                this.numOfSendingThreads = Integer.parseInt(temp);
            } catch (NumberFormatException e) {
                throw new ParseException(charonConfigFileName + " : The number of sendingThreads value must be an integer.)", 0);
            }
        } else {
            this.numOfSendingThreads = DEFAULT_NUMBER_OF_SENDING_THREADS;
        }

        temp = props.getProperty(TAG_NUMBER_OF_PREFETCHINGG_THREADS);
        if (temp != null && !temp.equals("")) {
            temp = solveVariables(props, CharonConstants.CHARON_CONFIG_FILE_NAME, temp);
            try {
                this.numOfPrefetchingThreads = Integer.parseInt(temp);
            } catch (NumberFormatException e) {
                throw new ParseException(charonConfigFileName + " : The number of prefetching threads value must be an integer.)", 0);
            }
        } else {
            this.numOfPrefetchingThreads = DEFAULT_NUMBER_OF_PREFETCHINGG_THREADS;
        }

        temp = props.getProperty(TAG_NUMBER_DEPSKY_DATA_THREADS);
        if (temp != null && !temp.equals("")) {
            temp = solveVariables(props, CharonConstants.CHARON_CONFIG_FILE_NAME, temp);
            try {
                this.numOfDepSkyDataThreads = Integer.parseInt(temp);
            } catch (NumberFormatException e) {
                throw new ParseException(charonConfigFileName + " : The number of DepSky data threads value must be an integer.)", 0);
            }
        } else {
            this.numOfDepSkyDataThreads = DEFAULT_NUMBER_DEPSKY_DATA_THREADS;
        }

        temp = props.getProperty(TAG_NUMBER_DEPSKY_METADATA_THREADS);
        if (temp != null && !temp.equals("")) {
            temp = solveVariables(props, CharonConstants.CHARON_CONFIG_FILE_NAME, temp);
            try {
                this.numOfDepSkyMetadataThreads = Integer.parseInt(temp);
            } catch (NumberFormatException e) {
                throw new ParseException(charonConfigFileName + " : The number of DepSky metadata threads value must be an integer.)", 0);
            }
        } else {
            this.numOfDepSkyMetadataThreads = DEFAULT_NUMBER_DEPSKY_METADATA_THREADS;
        }

        temp = props.getProperty(TAG_TIMES);
        if (temp != null && !temp.equals("")) {
            temp = solveVariables(props, CharonConstants.CHARON_CONFIG_FILE_NAME, temp);
            if (temp.equalsIgnoreCase("true") || temp.equalsIgnoreCase("false")) {
                this.times = temp.equalsIgnoreCase("true");
            } else {
                throw new ParseException(charonConfigFileName + " : The " + TAG_TIMES + " value must be a boolean.)", 0);
            }
        } else {
            this.times = DEFAULT_TIMES;
        }

        ///////////////////////////
        temp = props.getProperty(TAG_LOCAL_REP_SERVER_ADDR);
        if (temp != null && !temp.equals("")) {
            this.localRepositoryServerAddress = solveVariables(props, CharonConstants.CHARON_CONFIG_FILE_NAME, temp);
        } else {
            throw new ParseException(charonConfigFileName + " : The " + TAG_LOCAL_REP_SERVER_ADDR + " value must be in an IP:PORT format.)", 0);
        }
        /////////////////////////

        temp = props.getProperty(TAG_MOUNT_POINT);
        if (temp != null && !temp.equals("")) {
            this.mountPoint = solveVariables(props, CharonConstants.CHARON_CONFIG_FILE_NAME, temp);
        } else {
            throw new ParseException(charonConfigFileName + " : The mountPoint value must be specified: (" + TAG_MOUNT_POINT + "=[mountPoint])", 0);
        }

        temp = props.getProperty(TAG_CLIENT_NAME);
        if (temp != null && !temp.equals("")) {
            this.clientName = solveVariables(props, CharonConstants.CHARON_CONFIG_FILE_NAME, temp);
        } else {
            throw new ParseException(charonConfigFileName + " : The mountPoint value must be specified: (" + TAG_MOUNT_POINT + "=[mountPoint])", 0);
        }

        temp = props.getProperty(TAG_EMAIL);
        if (temp != null && !temp.equals("")) {
            this.email = solveVariables(props, CharonConstants.CHARON_CONFIG_FILE_NAME, temp);
        } else {
            throw new ParseException(charonConfigFileName + " : The email value must be specified: (" + email + "=[email])", 0);
        }

        //		temp = props.getProperty(TAG_LOCAL_REPOSITORY_DIRECTORY);
        //		if(temp != null && !temp.equals(""))
        //			this.localRepositoryDirectory = solveVariables(props, temp);
        //		else
        //			throw new ParseException( charonConfigFileName +" : The path to Local Repository Directory must be specified: ("+TAG_LOCAL_REPOSITORY_DIRECTORY+"=[pathToLocalRepositoryDirectory])",0);
        //		temp = props.getProperty(TAG_REMOTE_REPOSITORY_DIRECTORY);
        //		if(temp != null && !temp.equals(""))
        //			this.remoteRepositoryDirectory = solveVariables(props, temp);
        //		else
        //			throw new ParseException( charonConfigFileName +" : The path to Remote Repository Directory must be specified: ("+TAG_LOCAL_REPOSITORY_DIRECTORY+"=[pathToRemoteRepositoryDirectory])",0);
        temp = props.getProperty(TAG_CACHE_DIRECTORY);
        if (temp != null && !temp.equals("")) {
            this.cacheDirectory = solveVariables(props, CharonConstants.CHARON_CONFIG_FILE_NAME, temp);
        } else {
            throw new ParseException(charonConfigFileName + " : The path to Cache Directory must be specified: (" + TAG_CACHE_DIRECTORY + "=[pathToCacheDirectory])", 0);
        }

        temp = props.getProperty(TAG_SHARE_TOKENS_FOLDER);
        if (temp != null && !temp.equals("")) {
            this.shareTokensDir = solveVariables(props, CharonConstants.CHARON_CONFIG_FILE_NAME, temp);
        } else {
            this.shareTokensDir = DEFAULT_SHARE_TOKENS_FOLDER;
        }

        temp = props.getProperty(TAG_SITE_IDS_FOLDER);
        if (temp != null && !temp.equals("")) {
            this.siteIdsDir = solveVariables(props, CharonConstants.CHARON_CONFIG_FILE_NAME, temp);
        } else {
            this.siteIdsDir = DEFAULT_SITE_IDS_FOLDER;
        }

        temp = props.getProperty(TAG_FSYNC_TO_CLOUDS);
        if (temp != null && !temp.equals("")) {
            temp = solveVariables(props, CharonConstants.CHARON_CONFIG_FILE_NAME, temp);
            if (temp.equalsIgnoreCase("true") || temp.equalsIgnoreCase("false")) {
                this.fsyncToCloud = Boolean.parseBoolean(temp);
            } else {
                throw new ParseException(charonConfigFileName + " : The " + TAG_FSYNC_TO_CLOUDS + " value must be a boolean.)", 0);
            }
        } else {
            this.fsyncToCloud = DEFAULT_FSYNC_TO_CLOUDS;
        }

        temp = props.getProperty(TAG_USE_MAIN_MEMORY_CACHE);
        if (temp != null && !temp.equals("")) {
            temp = solveVariables(props, CharonConstants.CHARON_CONFIG_FILE_NAME, temp);
            if (temp.equalsIgnoreCase("true") || temp.equalsIgnoreCase("false")) {
                this.useMainMemoryCache = temp.equalsIgnoreCase("true");
            } else {
                throw new ParseException(charonConfigFileName + " : The " + TAG_USE_MAIN_MEMORY_CACHE + " value must be a boolean.)", 0);
            }
        } else {
            this.useMainMemoryCache = DEFAULT_USE_MAIN_MEMORY_CACHE;
        }

        temp = props.getProperty(TAG_PREFETCHING);
        if (temp != null && !temp.equals("")) {
            temp = solveVariables(props, CharonConstants.CHARON_CONFIG_FILE_NAME, temp);
            if (temp.equalsIgnoreCase("true") || temp.equalsIgnoreCase("false")) {
                this.prefecting = temp.equalsIgnoreCase("true");
            } else {
                throw new ParseException(charonConfigFileName + " : The " + TAG_USE_MAIN_MEMORY_CACHE + " value must be a boolean.)", 0);
            }
        } else {
            this.prefecting = DEFAULT_PREFETCHING;
        }

        temp = props.getProperty(TAG_DEBUG);
        if (temp != null && !temp.equals("")) {
            temp = solveVariables(props, CharonConstants.CHARON_CONFIG_FILE_NAME, temp);
            if (temp.equalsIgnoreCase("true") || temp.equalsIgnoreCase("false")) {
                this.debug = temp.equalsIgnoreCase("true");
            } else {
                throw new ParseException(charonConfigFileName + " : The " + TAG_DEBUG + " value must be a boolean.)", 0);
            }
        } else {
            this.debug = DEFAULT_DEBUG;
        }

        temp = props.getProperty(TAG_PNS_ID);
        if (temp != null && !temp.equals("")) {
            this.pnsId = solveVariables(props, CharonConstants.CHARON_CONFIG_FILE_NAME, temp);
            if(!this.pnsId.startsWith("charon"))
            	this.pnsId = "charon-" + clientId + "-pns-" + this.pnsId;
        } else {
            char[] randname = new char[10];
            for (int i = 0; i < 10; i++) {
                char rand = (char) (Math.random() * 26 + 'a');
                randname[i] = rand;
            }
            this.pnsId = "charon-" + clientId + "-pns-" + new String(randname);
            if (props.containsKey(TAG_PNS_ID)) {
                props.setProperty(TAG_PNS_ID, this.pnsId);
                updatePNSId(charonConfigFileName, pnsId, false);
            } else {
                updatePNSId(charonConfigFileName, pnsId, true);
                props.put(TAG_PNS_ID, this.pnsId);
            }

        }

    }

    private void readStorageLocationsConfiguration(String locationsConfigFileName, Properties configFileProps) throws ParseException, FileNotFoundException {
        this.cuesLocation = new HashMap<String, Location>();

        String locationsConfigFileContent = new Scanner(new File(locationsConfigFileName)).useDelimiter("\\Z").next();

        defaultLocation = null;

        StringTokenizer stoken = new StringTokenizer(locationsConfigFileContent, "\n");
        while (stoken.hasMoreTokens()) {
            String token = stoken.nextToken();
            if (token.startsWith(DEFAULT_LOCATION_TAG + "=")) {
                String value = token.split("=", 2)[1];
                switch (value) {
                    case CharonConstants.COC_TAG:
                        defaultLocation = Location.CoC;
                        break;
                    case CharonConstants.SINGLE_CLOUD_TAG:
                        defaultLocation = Location.SINGLE_CLOUD;
                        break;
                    case CharonConstants.PRIVATE_REP_TAG:
                        defaultLocation = Location.PRIVATE_REP;
                        break;
                    case CharonConstants.EXTERNAL_REP_TAG:
                        defaultLocation = Location.EXTERNAL_REP;
                        break;
                    default:
                        throw new ParseException("DEFAULT_LOCATION value is invalid. It must be a supported one (" + CharonConstants.COC_TAG + "," + CharonConstants.SINGLE_CLOUD_TAG + "," + CharonConstants.PRIVATE_REP_TAG + "," + CharonConstants.EXTERNAL_REP_TAG + ")", 0);
                }
                locationsConfigFileContent = locationsConfigFileContent.replace(token, "");
            }
        }
        if (defaultLocation == null) {
            throw new ParseException("You must specify a DEFAULT_LOCATION in locations.config file.", 0);
        }

        Map<String, String> labels = null;
        try {
            labels = readKeyValueLabelsFromLocationsFile(locationsConfigFileContent);
        } catch (ParseException e2) {
            throw new ParseException(e2.getMessage() + "[line: " + e2.getErrorOffset() + "]", 0);
        }

        for (Entry<String, String> e : labels.entrySet()) {
            String cue = e.getKey();
            String content = e.getValue();

            if (!content.contains("{") || !content.contains("{")) {
                throw new ParseException("Bad formated " + locationsConfigFileName + " file. (error in configuration block of label " + e.getKey() + " )", 0);
            }

            Location local = null;
            if (content.startsWith(CharonConstants.COC_TAG)) {
                local = Location.CoC;
            } else if (content.startsWith(CharonConstants.SINGLE_CLOUD_TAG)) {
                local = Location.SINGLE_CLOUD;
            } else if (content.startsWith(CharonConstants.PRIVATE_REP_TAG)) {
                local = Location.PRIVATE_REP;
            } else if (content.startsWith(CharonConstants.EXTERNAL_REP_TAG)) {
                local = Location.EXTERNAL_REP;
            } else {
                throw new ParseException("[Label: " + e.getKey() + "], invalid TAG.", 0);
            }

            content = content.substring(content.indexOf("{") + 1, content.lastIndexOf("}"));
            content = solveVariables(configFileProps, locationsConfigFileName, content);

            if (local == Location.CoC || local == Location.SINGLE_CLOUD) {
                boolean isSingleCloud = (local == Location.SINGLE_CLOUD);

                try {
                    List<SingleCloudConfiguration> l;
                    if (content.startsWith("file:")) {
                        l = readDepSkyCredentials(content.substring("file:".length()), true, (local == Location.SINGLE_CLOUD));
                    } else {
                        l = readDepSkyCredentials(content, false, (local == Location.SINGLE_CLOUD));
                    }

                    if (isSingleCloud) {
                        if (singleCloudConfig != null) {
                            throw new ParseException("(-) Only one configuration is allowed for the location: " + local, 0);
                        }
                        if (l.size() != 1) {
                            throw new ParseException("", 0);
                        }
                        singleCloudConfig = l.get(0);
                        cuesLocation.put(cue, singleCloudConfig.getLocation());
                    } else {
                        if (cocConfig != null) {
                            throw new ParseException("(-) Only one configuration is allowed for the location: " + local, 0);
                        }
                        cocConfig = new CoCConfiguration(l);
                        cuesLocation.put(cue, cocConfig.getLocation());
                    }
                } catch (ParseException e1) {
                    throw new ParseException(e1.getMessage() + "[Label: " + e.getKey() + "]", 0);
                } catch (FileNotFoundException e1) {
                    throw new ParseException("Bad format in " + locationsConfigFileName + " file. File not found [" + content + "]", 0);
                }
            } else if (local == Location.PRIVATE_REP || local == Location.EXTERNAL_REP) {

                Scanner sc = null;

                if (content.startsWith("file:")) {
                    sc = new Scanner(new File(content.substring("file:".length())));
                } else {
                    sc = new Scanner(content);
                }

                String path = null;
                while (path == null && sc.hasNext()) {
                    path = sc.nextLine();
                    if (path.startsWith("#") || path.equals("")) {
                        path = null;
                    }
                }
                if (path == null) {
                    sc.close();
                    throw new ParseException("[Label: " + e.getKey() + "] - bad path!", 0);
                }

                if (content.startsWith("file:")) {
                    path = solveVariables(configFileProps, content.substring("file:".length()), path);
                }

                sc.close();

                if (local == Location.PRIVATE_REP) {
                    if (privateRepConfig != null) {
                        throw new ParseException("(-) Only one configuration is allowed for the location: " + local, 0);
                    }
                    this.privateRepConfig = new PrivateRepConfiguration(path);
                    cuesLocation.put(e.getKey(), privateRepConfig.getLocation());
                } else {
                    if (externalRepConfig != null) {
                        throw new ParseException("(-) Only one configuration is allowed for the location: " + local, 0);
                    }
                    this.externalRepConfig = new ExternalRepConfiguration(path);
                    cuesLocation.put(e.getKey(), externalRepConfig.getLocation());
                }
            }
        }

        AStorageConfiguration defaultConf = null;
        switch (defaultLocation) {
            case CoC:
                defaultConf = cocConfig;
                break;
            case SINGLE_CLOUD:
                defaultConf = singleCloudConfig;
                break;
            case PRIVATE_REP:
                defaultConf = privateRepConfig;
                break;
            case EXTERNAL_REP:
                defaultConf = externalRepConfig;
                break;
            default:
                throw new ParseException("DEFAULT_LOCATION value is invalid. It must be a supported one (" + CharonConstants.COC_TAG + "," + CharonConstants.SINGLE_CLOUD_TAG + "," + CharonConstants.PRIVATE_REP_TAG + "," + CharonConstants.EXTERNAL_REP_TAG + ")", 0);
        }
        if (defaultConf == null) {
            throw new ParseException("(-) You must specify at least the configuration of your default location. ", 0);
        }

    }

    private String solveVariables(Properties props, String charonConfigFileName, String value) throws ParseException {
        String originalValue = value;
        if (value.contains("{")) {
            if (value.contains("}")) {
                String var = value.substring(value.indexOf("{") + 1, value.indexOf("}"));
                String newValue = props.getProperty(var);
                value = value.replace("{" + var + "}", newValue);
                if (value.contains("{") || value.contains("}")) {
                    try {
                        value = solveVariables(props, charonConfigFileName, value);
                    } catch (ParseException e) {
                        throw new ParseException(charonConfigFileName + " : Wrong use of variable. [" + originalValue + "]", 0);
                    }
                }
                return value;
            } else {
                throw new ParseException(charonConfigFileName + " : Wrong use of variable. [" + originalValue + "]", 0);
            }
        }
        if (value.contains("}")) {
            throw new ParseException(charonConfigFileName + " : Wrong use of variable. [" + originalValue + "]", 0);
        }
        return value;
    }

    public String getClientName() {
        return clientName;
    }

    public Location getDefaultLocation() {
        return defaultLocation;
    }

    public int getRemoteRepositoryServerPort() {
        if (localRepositoryServerAddress.contains(":")) {
            return Integer.parseInt(localRepositoryServerAddress.split(":", 2)[1]);
        } else {
            return Integer.parseInt(localRepositoryServerAddress);
        }
    }

    public boolean isTimes() {
        return times;
    }

    public boolean useCompression() {
        return compression;
    }

    //	public List<String> getCues(){
    //		List<String> res = new ArrayList<String>();
    //		for(String s : storageConfiguration.keySet())
    //			res.add(s);
    //
    //		return res;
    //	}
    public String getPrivateRepositoryDirectory() {
        return privateRepConfig.getPath();
    }

    public PrivateRepConfiguration getPrivateRepConfig() {
        return privateRepConfig;
    }

    public ExternalRepConfiguration getExternalRepConfig() {
        return externalRepConfig;
    }
    
    public String getExternalRepositoryDirectory() {
        return externalRepConfig.getPath();
    }

    //	public String getRemoteRepositoryDirectory() {
    //		return remoteRepositoryDirectory;
    //	}
    public String getCacheDirectory() {
        return cacheDirectory;
    }

    public boolean isUseMainMemoryCache() {
        return useMainMemoryCache;
    }

    public String getMyEmail() {
        return email;
    }

    public boolean isDebug() {
        return debug;
    }

    public int getNumOfSendingThreads() {
        return numOfSendingThreads;
    }

    public int getNumOfDepSkyDataThreads() {
        return numOfDepSkyDataThreads;
    }

    public int getNumOfDepSkyMetadataThreads() {
        return numOfDepSkyMetadataThreads;
    }

    public int getNumOfPrefetchingThreads() {
        return numOfPrefetchingThreads;
    }

    public boolean isFsyncToCloud() {
        return fsyncToCloud;
    }

    public String getMountPoint() {
        return mountPoint;
    }

    public int getClientId() {
        return clientId;
    }

    public String getPnsId() {
        return pnsId;
    }

    public boolean usePrefecting() {
        return prefecting;
    }

    public String getShareTokensDir() {
        return shareTokensDir;
    }

    public String getSiteIdsDir() {
        return siteIdsDir;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb = sb.append("##### TAGS #####\n");
        sb = sb.append(TAG_MOUNT_POINT).append("=").append(mountPoint).append("\n");
        sb = sb.append(TAG_CLIENT_ID).append("=").append(clientId).append("\n");

        //		sb = sb.append(TAG_LOCAL_REPOSITORY_DIRECTORY).append("=").append(localRepositoryDirectory).append("\n");
        //		sb = sb.append(TAG_REMOTE_REPOSITORY_DIRECTORY).append("=").append(remoteRepositoryDirectory).append("\n");
        sb = sb.append(TAG_CACHE_DIRECTORY).append("=").append(cacheDirectory).append("\n");

        sb = sb.append(TAG_PREFETCHING).append("=").append(prefecting).append("\n");

        sb = sb.append(TAG_USE_MAIN_MEMORY_CACHE).append("=").append(useMainMemoryCache).append("\n");
        sb = sb.append(TAG_FSYNC_TO_CLOUDS).append("=").append(fsyncToCloud).append("\n");
        sb = sb.append(TAG_DEBUG).append("=").append(debug).append("\n");
        sb = sb.append(TAG_LOCAL_REP_SERVER_ADDR).append("=").append(localRepositoryServerAddress).append("\n");

        sb = sb.append(TAG_PNS_ID).append("=").append(pnsId).append("\n\n");

        sb = sb.append("##### STORAGE CONFIGURATION #####\n");

        for (Entry<String, Location> l : cuesLocation.entrySet()) {
            switch (l.getValue()) {
                case CoC:
                    sb = sb.append(l.getKey()).append("=").append(cocConfig.toString()).append("\n\n");
                    break;
                case SINGLE_CLOUD:
                    sb = sb.append(l.getKey()).append("=").append(singleCloudConfig.toString()).append("\n\n");
                    break;
                case PRIVATE_REP:
                    sb = sb.append(l.getKey()).append("=").append(privateRepConfig.toString()).append("\n\n");
                    break;
                case EXTERNAL_REP:
                    sb = sb.append(l.getKey()).append("=").append(externalRepConfig.toString()).append("\n\n");
                    break;
                default:
                    break;
            }
        }

        return sb.toString();

    }

    private void updatePNSId(String charonConfigFileName, String pnsId, boolean newLine) {
        File f = new File(charonConfigFileName);
        try {
            if (newLine) {
                FileWriter fw = new FileWriter(f, true);
                fw.append("\n" + TAG_PNS_ID + "=" + pnsId);
                fw.close();
            } else {
                Scanner sc = new Scanner(f);
                String content = new String();
                while (sc.hasNext()) {
                    content = content.concat(sc.nextLine()).concat("\n");
                }
                sc.close();
                content = content.replace(TAG_PNS_ID + "=", TAG_PNS_ID + "=" + pnsId);
                FileWriter fw = new FileWriter(f, false);
                fw.write(content);
                fw.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String findEndBlock(Scanner sc) {
        String res = "";
        while (sc.hasNext() && !res.contains("}")) {
            if (res.contains("{")) {
                return null;
            }
            res = res.concat(sc.nextLine()).concat("\n");
        }

        if (res.contains("{")) {
            return null;
        }

        res = res.substring(0, res.length() - 1);
        if (res.endsWith("}")) {
            return res;
        } else {
            return null;
        }
    }

    private Map<String, String> readKeyValueLabelsFromLocationsFile(String locationsConfigContent) throws FileNotFoundException, ParseException {
        Scanner sc = new Scanner(locationsConfigContent);
        Map<String, String> labels = new HashMap<String, String>();
        String line;
        String block;
        int lineNum = 0;
        while (sc.hasNext()) {
            lineNum++;
            line = sc.nextLine();
            if (line.startsWith("#") || line.equals("")) {
                continue;
            }

            String[] sx = line.split("=", 2);
            if (sx.length != 2 || sx[0].equals("")) {
                throw new ParseException("Bad formated " + locationsConfigContent + " file.", lineNum);
            }

            block = sx[1];
            if (block.startsWith("local")) {
                if (block.equals("local")) {
                    labels.put(sx[0], block);
                    continue;
                } else {
                    throw new ParseException("Bad formated " + locationsConfigContent + " file.", lineNum);
                }
            }
            if (!block.contains("}")) {
                block = block.concat("\n");
                String res = findEndBlock(sc);
                if (res != null) {
                    labels.put(sx[0], block.concat(res));
                    for (int i = 0; i < res.length(); i++) {
                        if (res.charAt(i) == '\n') {
                            lineNum++;
                        }
                    }
                    lineNum++;
                    continue;
                } else {
                    throw new ParseException("Bad formated " + locationsConfigContent + " file. (error in configuration block of label " + sx[0] + " )", lineNum);
                }
            } else {
                if (!block.endsWith("}")) {
                    throw new ParseException("Bad formated " + locationsConfigContent + " file. (error in configuration block of label " + sx[0] + " )", lineNum);
                }
                labels.put(sx[0], block);
                continue;
            }
        }

        return labels;
    }

    private List<SingleCloudConfiguration> readDepSkyCredentials(String content, boolean isFile, boolean isSingleCloud) throws ParseException, FileNotFoundException {
        Scanner sc = null;
        if (isFile) {
            sc = new Scanner(new File(content));
        } else {
            sc = new Scanner(content);
        }

        String line;
        String[] splitLine;
        Properties props = null;
        LinkedList<SingleCloudConfiguration> list = new LinkedList<SingleCloudConfiguration>();
        int lineNum = -1;
        String driverType = null, driverId = null, accessKey = null, secretKey = null, location = null;
        while (sc.hasNext()) {
            lineNum++;
            line = sc.nextLine();
            if (line.startsWith("#") || line.equals("")) {
                continue;
            }

            splitLine = line.split("=", 2);
            if (splitLine.length != 2) {
                sc.close();
                throw new ParseException("Bad formated " + (isSingleCloud ? "SingleCloud" : "DepSky") + " credentials.", lineNum);
            }

            try {
                if (splitLine[0].equals("driver.type")) {
                    if (driverType != null) {
                        list.add(getSingleCloudConf(driverType, driverId, accessKey, secretKey, location, props, isSingleCloud));
                        driverType = driverId = accessKey = secretKey = location = null;
                        props = null;
                    }
                    driverType = splitLine[1];

                } else if (splitLine[0].equals("driver.id")) {
                    if (driverId != null) {
                        list.add(getSingleCloudConf(driverType, driverId, accessKey, secretKey, location, props, isSingleCloud));
                        driverType = driverId = accessKey = secretKey = location = null;
                        props = null;
                    }
                    driverId = splitLine[1];

                } else if (splitLine[0].equals("accessKey")) {
                    if (accessKey != null) {
                        list.add(getSingleCloudConf(driverType, driverId, accessKey, secretKey, location, props, isSingleCloud));
                        driverType = driverId = accessKey = secretKey = location = null;
                        props = null;
                    }
                    accessKey = splitLine[1];
                } else if (splitLine[0].equals("secretKey")) {
                    if (secretKey != null) {
                        list.add(getSingleCloudConf(driverType, driverId, accessKey, secretKey, location, props, isSingleCloud));
                        driverType = driverId = accessKey = secretKey = location = null;
                        props = null;
                    }
                    secretKey = splitLine[1];
                } else if (splitLine[0].equals("location")) {
                    if (location != null) {
                        list.add(getSingleCloudConf(driverType, driverId, accessKey, secretKey, location, props, isSingleCloud));
                        driverType = driverId = accessKey = secretKey = location = null;
                        props = null;
                    }
                    location = splitLine[1];
                } else {
                    if (props == null) {
                        props = new Properties();
                    }
                    props.put(splitLine[0], splitLine[1]);
                }
            } catch (ParseException p) {
                sc.close();
                throw new ParseException("Bad formated " + (isSingleCloud ? "SingleCloud" : "DepSky") + " credentials.", lineNum);
            }

        }
        try {
            list.add(getSingleCloudConf(driverType, driverId, accessKey, secretKey, location, props, isSingleCloud));
        } catch (ParseException p) {
            sc.close();
            throw new ParseException("Bad formated DepSky credentials.", lineNum);
        }
        sc.close();
        return list;
    }

    private SingleCloudConfiguration getSingleCloudConf(String driverType, String driverId, String accessKey, String secretKey, String location, Properties props, boolean isSinlgeCloud) throws ParseException {
        if (driverType != null && (isSinlgeCloud || driverId != null) && accessKey != null && secretKey != null) {
            SingleCloudConfiguration res = new SingleCloudConfiguration(driverType, driverId, accessKey, secretKey, location);
            res.setOtherParams(props);
            return res;
        } else {
            throw new ParseException("", 0);
        }
    }

    //	private boolean isInteger(String s){
    //		try{
    //			Integer.parseInt(s);
    //			return true;
    //		}catch(NumberFormatException e){
    //			return false;
    //		}
    //	}
    public Location getCueLocation(String cue) {
        return cuesLocation.get(cue);
    }

    public CoCConfiguration getCoCConfiguration() {
        return cocConfig;
    }

    public SingleCloudConfiguration getSingleCloudConfig() {
        return singleCloudConfig;
    }

    //	private RemoteConfiguration readRemoteConfiguration(String content, boolean isFile, String locationsConfigFileName) throws FileNotFoundException, ParseException{
    //		Scanner sc = null;
    //		if(isFile)
    //			sc = new Scanner(new File(content));
    //		else
    //			sc = new Scanner(content);
    //	
    //		List<RemoteLocationEntry> result = new ArrayList<RemoteLocationEntry>();
    //	
    //		String line;
    //		int lineNum = -1;
    //		String [] splitLine;
    //		while(sc.hasNext()){
    //			lineNum++;
    //			line=sc.nextLine();
    //			if(line.startsWith("#") || line.equals(""))
    //				continue;
    //			splitLine = line.split(":");
    //			if(splitLine.length != 3){
    //				sc.close();
    //				throw new ParseException("Bad formated RemoteConfigurations in " + locationsConfigFileName + " file. Please use de format [id]:[ip]:[port]", lineNum);
    //			}
    //			if(!isInteger(splitLine[0]) || !isInteger(splitLine[2])){
    //				sc.close();
    //				throw new ParseException("Bad formated RemoteConfiguration in " + locationsConfigFileName + ". Bad id or port.", lineNum);
    //			}
    //			result.add(new RemoteLocationEntry(Integer.parseInt(splitLine[0]), splitLine[1], Integer.parseInt(splitLine[2])));
    //	
    //		}
    //		sc.close();
    //		return new RemoteConfiguration(result);
    //	}
}
