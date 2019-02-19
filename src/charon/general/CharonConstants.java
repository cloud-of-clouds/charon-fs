package charon.general;

/**
 * 
 */

/**
 * @author rmendes
 *
 */
public class CharonConstants {

	public static final int LOCK_RETRIES = 1;
	
	public static final String LOCK_PREFIX = "Lock_";
	
	public static final String CREDENTIALS_FILE_NAME = "config/credentials.charon";
	public static final String CHARON_CONFIG_FILE_NAME = "config/charon.config";
	public static final String LOCATIONS_CONFIG_FILE_NAME = "config/locations.config";
	public static final String HDFS_CONF_LOCATION_CONFIG_FILE_NAME = "config/hopsfsRep.config";
		
	
	public static final String SNS_FOLDER = "NewSNSs";
	public static final String SITE_IDS_FOLDER = "NewSiteIds";
	
	public static final String TAG_NS_NAMES = "--ns";
	public static final String TAG_CHARON_CONFIG_FILE = "--configFile";
	public static final String TAG_LOCATIONS_FILE = "--locationsFile";
	
	public static final int BLOCK_SIZE = 512;
	
	// Open flags mask 
	public static final int O_ACCMODE = 0x400003;
	// ++++++++
	
	// File type Modes
	public static final int S_IFMT = 0x0170000; /* type of file */
	public static final int S_IFLNK = 0x0120000; /* symbolic link */
	public static final int S_IFREG = 0x0100000; /* regular */
	public static final int S_IFBLK = 0x0060000; /* block special */
	public static final int S_IFDIR = 0x0040000; /* directory */
	public static final int S_IFCHR = 0x0020000; /* character special */
	public static final int S_IFIFO = 0x0010000; /* this is a FIFO */
	public static final int S_ISUID = 0x0004000; /* set user id on execution */
	// ++++++++

	// Access permitions mask
	public static final int S_IRWXU   = 00700;         /* owner:  rwx------ */
	public static final int S_IRUSR   = 00400;         /* owner:  r-------- */
	public static final int S_IWUSR   = 00200;         /* owner:  -w------- */
	public static final int S_IXUSR   = 00100;         /* owner:  --x------ */

	public static final int S_IRWXG   = 00070;         /* group:  ---rwx--- */
	public static final int S_IRGRP   = 00040;         /* group:  ---r----- */
	public static final int S_IWGRP   = 00020;         /* group:  ----w---- */
	public static final int S_IXGRP   = 00010;         /* group:  -----x--- */

	public static final int S_IRWXO   = 00007;         /* others: ------rwx */
	public static final int S_IROTH   = 00004;         /* others: ------r-- */ 
	public static final int S_IWOTH   = 00002;         /* others: -------w- */
	public static final int S_IXOTH   = 00001;         /* others: --------x */
	// ++++++++

	
	public static final String PRIVATE_REP_TAG = "privateRep";
	public static final String EXTERNAL_REP_TAG = "externalRep";
	public static final String SINGLE_CLOUD_TAG = "cloud";
	public static final String COC_TAG = "coc";

	public static final int  ADD_EXTERNAL_PORT = 9016;

}
