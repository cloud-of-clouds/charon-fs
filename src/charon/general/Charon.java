package charon.general;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import charon.configuration.CharonConfiguration;
import charon.configuration.Location;
import charon.configuration.storage.remote.RemoteLocationEntry;
import charon.directoryService.DirectoryServiceImpl;
import charon.directoryService.NameSpace;
import charon.directoryService.NodeMetadata;
import charon.directoryService.NodeType;
import charon.directoryService.exceptions.DirectoryServiceException;
import charon.directoryService.externalManagement.ExternalFoldersToManageReceiverThread;
import charon.directoryService.externalManagement.ExternalManegementThread;
import charon.lockService.LockService;
import charon.storageService.StorageService;
import charon.storageService.repositories.HopsFSConnectionFactory;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;
import depsky.util.Pair;
import fuse.Filesystem3;
import fuse.FuseDirFiller;
import fuse.FuseException;
import fuse.FuseGetattrSetter;
import fuse.FuseMount;
import fuse.FuseOpenSetter;
import fuse.FuseSizeSetter;
import fuse.FuseStatfs;
import fuse.FuseStatfsSetter;
import fuse.XattrLister;
import fuse.XattrSupport;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.FileUtils;

/**
 * Cloud-of-clouds file system implementation.
 *
 * @author rmendes (rmendes@lasige.di.fc.ul.pt)
 * @author toliveira (toliveira@lasige.di.fc.ul.pt)
 *
 */
public class Charon implements Filesystem3, XattrSupport {

	private int numRead;
	private String lastReadPath;

	private int numWrite;
	private String lastWritePath;

	private final String CUE_PREFIX = ".site=";
	private static int clientId;
	private FuseStatfs statfs;
	private StorageService daS;
	private DirectoryServiceImpl directoryService;
	private LockUpdterNSSwitcher lockService;

	private Map<String, Integer> lockedFiles;
	private CharonConfiguration config;
	private NodeMetadata statistics;

	private static int fileNum;

	public static Map<String, Pair<String, Long>> times;

	public Charon(CharonConfiguration config, String[] addSNS) {

		numRead = 0;
		lastReadPath = null;

		numWrite = 0;
		lastWritePath = null;

		times = new HashMap<String, Pair<String, Long>>();
		this.config = config;
		System.out.println("- Mounting Charon.");
		Statistics.reset();

		Printer.setPrintAuth(config.isDebug());
		clientId = config.getClientId();

		try {
			System.out.print("-  Starting DepSky.");

			lockService = new LockUpdterNSSwitcher();
			System.out.print(".");
			directoryService = new DirectoryServiceImpl(config, clientId, lockService);
			System.out.print(".");
			daS = new StorageService(clientId, config, config.getNumOfSendingThreads(), 0, directoryService, lockService);
			System.out.println("done.");
			//            ((DirectoryServiceImpl) directoryService).setStorageService(daS);

		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		}

		new AddSiteIdsAndSNSsThread(directoryService, config.getShareTokensDir(), config.getSiteIdsDir()).start();

		statistics = NodeMetadata.getDefaultNodeMetadata("/", ".statistics.txt", NodeType.FILE, Long.parseLong("123412341234"), "123412341234", config.getDefaultLocation());
		statistics.setMode(33204);
		statistics.setRdev(0);
		fileNum = 0;

		statfs = new FuseStatfs();
		statfs.blocks = 0;
		statfs.blockSize = CharonConstants.BLOCK_SIZE;
		statfs.blocksFree = Integer.MAX_VALUE;
		statfs.files = 0;
		statfs.filesFree = 0;
		statfs.namelen = 2048;
		lockedFiles = new HashMap<String, Integer>();

		daS.recover();

		ExternalManegementThread extManagementThread = new ExternalManegementThread(10000, directoryService);
		new ExternalFoldersToManageReceiverThread(extManagementThread, directoryService).start();
		extManagementThread.start();

		System.out.println("\nCharon was mounted Successfully.");
	}

	@Override
	public int getattr(String path, FuseGetattrSetter getattrSetter) throws FuseException {
		Printer.println("\n :::: GETATTR( " + path + " )", "amarelo");
		Statistics.execOp(Statistics.GETATTR);
		
		if (path.equals("/.statistics.txt")) {
			getattrSetter.set(statistics.getInode(), statistics.getMode(), statistics.getNlink(), statistics.getUid(), statistics.getGid(),
					statistics.getRdev(), Statistics.getReport().length(), statistics.getBlocks(), (int) statistics.getAtime(),
					(int) statistics.getMtime(), (int) statistics.getCtime());
			return 0;
		}

		String pathWithoutCues = path;
		String[] splitPath = path.split("/");
		if (splitPath.length >= 2) {
			String newPath = "";
			for (int i = 1; i < splitPath.length; i++) {
				if (splitPath[i].toLowerCase().startsWith(CUE_PREFIX.toLowerCase())) {
					String location = splitPath[i].substring(CUE_PREFIX.length());
					if (config.getCueLocation(location) == null) {
						newPath = newPath.concat("/").concat(splitPath[i]);
					}
				} else {
					newPath = newPath.concat("/").concat(splitPath[i]);
				}
			}
			pathWithoutCues = newPath.equals("") ? "/" : newPath;
		}

		NodeMetadata metadata = null;
		long time = System.currentTimeMillis();
		try {
			metadata = directoryService.getMetadata(pathWithoutCues);
		} catch (DirectoryServiceException e1) {
			if (path.equals("/.statistics.txt")) {
				getattrSetter.set(statistics.getInode(), statistics.getMode(), statistics.getNlink(), statistics.getUid(), statistics.getGid(),
						statistics.getRdev(), Statistics.getReport().length(), statistics.getBlocks(), (int) statistics.getAtime(),
						(int) statistics.getMtime(), (int) statistics.getCtime());
				return 0;
			} else {
				throw new FuseException(e1.getMessage()).initErrno(FuseException.ENOENT);
			}
		}
		Statistics.getMeta(System.currentTimeMillis() - time);

		getattrSetter.set(metadata.getInode(), metadata.getMode(), metadata.getNlink(), Integer.parseInt(System.getProperty("uid")), Integer.parseInt(System.getProperty("gid")),
				metadata.getRdev(), metadata.getSize(), metadata.getBlocks(), (int) metadata.getAtime(),
				(int) metadata.getMtime(), (int) metadata.getCtime());
		return 0;
	}

	@Override
	public int getdir(String path, FuseDirFiller dirFiller) throws FuseException {
		Printer.println("\n :::: GETDIR( " + path + " )", "amarelo");
		Statistics.execOp(Statistics.GETDIR);

		long time = System.currentTimeMillis();
		directoryService.getNodeChildren(path, dirFiller);

		Statistics.getDirMeta(System.currentTimeMillis() - time);

		if (path.equals("/")) {
			dirFiller.add(".statistics.txt", statistics.getInode(), statistics.getMode());
		}
		return 0;
	}

	@Override
	public int mkdir(String path, int mode) throws FuseException {
		Printer.println("\n :::: MKDIR( " + path + " )", "amarelo");
		Statistics.execOp(Statistics.MKDIR);

		String cue = null;

		// CUES //
		String pathWithoutCues = path;
		String[] splitPath = path.split("/");
		if (splitPath.length >= 2) {
			String newPath = "";
			for (int i = 1; i < splitPath.length; i++) {
				if (splitPath[i].toLowerCase().startsWith(CUE_PREFIX.toLowerCase())) {
					String location = splitPath[i].substring(CUE_PREFIX.length());
					if (config.getCueLocation(location) != null) {
						if (cue != null) {
							throw new FuseException("multiple location cue definition").initErrno(FuseException.ENOTSUPP);
						}
						cue = location;
					} else {
						newPath = newPath.concat("/").concat(splitPath[i]);
					}
				} else {
					newPath = newPath.concat("/").concat(splitPath[i]);
				}
			}
			pathWithoutCues = newPath.equals("") ? "/" : newPath;
		}

		//		System.out.println("CUE = " + cue);
		String[] vec = dividePath(pathWithoutCues);

		NameSpace ns = directoryService.getNS(vec[0]);
		NodeMetadata meta = ns.getMetadata(vec[0]);
		if (meta.isExternalManaged()) {
			throw new FuseException("Cannot delete directories inside .external directory.").initErrno(FuseException.EOPNOTSUPP);
		}

		accessControl(ns);

		// cannot set the location data inside.
		if (cue != null && !directoryService.isPrivate(ns)) {
			throw new FuseException("Cannot set the location of an sns.").initErrno(FuseException.EAFNOSUPPORT);
		}

		//TODO: nao podemos mudar a localização dos dados dentro de pastas com localizações ja definidas.
		// Solution: é por a location da pasta raiz a null, e so permitir criar a pasta se nao houver cue ou caso a cue seja identica à da pasta mae.
		Location l = ns.getMetadata(vec[0]).getLocation();

		if (cue != null) {
			l = config.getCueLocation(cue);
		}

		switch (l) {
		case EXTERNAL_REP:
			if (!HopsFSConnectionFactory.mountHDFSsuccess) {
				throw new FuseException("You cannot create and 'external' Dir because "
						+ "the external repository is not mounted. Please configure "
						+ "the file config/locations.config and config/hopsfsRep.config"
						+ "and re-run Charon!").initErrno(FuseException.EAFNOSUPPORT);
			}
		default:
			break;
		}

		// lock
		lock(ns);

		String idPath = getNextIdPath(ns);
		NodeMetadata m = NodeMetadata.getDefaultNodeMetadata(vec[0], vec[1], NodeType.DIR, System.currentTimeMillis() + fileNum, idPath, l);

		long time = System.currentTimeMillis();
		directoryService.putMetadata(m, ns);
		Statistics.putMeta(System.currentTimeMillis() - time);

		return 0;
	}

	@Override
	public int mknod(String path, int mode, int rdev) throws FuseException {
		printRW();
		Printer.println("\n :::: MKNOD( " + path + ", " + mode + " )", "amarelo");
		Statistics.execOp(Statistics.MKNOD);

		String[] splitPath = path.split("/");
		if (splitPath[splitPath.length - 1].toLowerCase().startsWith(CUE_PREFIX.toLowerCase())) {
			throw new FuseException("File name already reserved (as a location cue).").initErrno(FuseException.EALREADY);
		}

		String[] vec = dividePath(path);
		NameSpace ns = directoryService.getNS(vec[0]);

		accessControl(ns);

		String idPath = getNextIdPath(ns);
		NodeMetadata mParent = ns.getMetadata(vec[0]);

		if (mParent.isExternalManaged()) {
			throw new FuseException("Cannot delete directories inside .external directory.").initErrno(FuseException.EOPNOTSUPP);
		}

		lock(ns);

		NodeMetadata m = NodeMetadata.getDefaultNodeMetadata(vec[0], vec[1], NodeType.FILE, System.currentTimeMillis() + fileNum, idPath, mParent.getLocation());

		printFileId(m);

		times.put(m.getPath(), new Pair<String, Long>(m.getName(), System.currentTimeMillis()));
		m.setMode(mode);
		m.setRdev(rdev);
		long time = System.currentTimeMillis();
		directoryService.putMetadata(m, ns);
		Statistics.putMeta(System.currentTimeMillis() - time);

		statfs.blocks = statfs.blocks + (CharonConstants.BLOCK_SIZE - 1) / CharonConstants.BLOCK_SIZE;

		return 0;
	}

	@Override
	public int open(String path, int flags, FuseOpenSetter openSetter) throws FuseException {
		printRW();
		Printer.println("\n :::: OPEN( " + path + ", " + (((flags & CharonConstants.O_ACCMODE) == O_RDWR) ? "read_write" : ((flags & CharonConstants.O_ACCMODE) == O_WRONLY) ? "write" : "read") + " )", "amarelo");
		Statistics.execOp(Statistics.OPEN);

		if (path.equals("/.statistics.txt")) {
			return 0;
		}

		NameSpace ns = directoryService.getNS(path);
		if (ns == null) {
			throw new FuseException("No such file.").initErrno(FuseException.ENOENT);
		}

		long time = System.currentTimeMillis();
		NodeMetadata nm = ns.getMetadata(path);
		if (nm.isExternalManaged() && ((flags & CharonConstants.O_ACCMODE) == O_WRONLY || (flags & CharonConstants.O_ACCMODE) == O_RDWR)) {
			throw new FuseException("Cannot open files to write inside .external directory.").initErrno(FuseException.EOPNOTSUPP);
		}

		Statistics.getMeta(System.currentTimeMillis() - time);

		if (!nm.isPrivate() && ((flags & CharonConstants.O_ACCMODE) == O_WRONLY || (flags & CharonConstants.O_ACCMODE) == O_RDWR)) {

			//access control
			accessControl(ns);

			//locking
			if (lockService.tryAcquire(nm.getIdpath(), LockService.LOCK_TIME)) {
				lockedFiles.put(nm.getIdpath(), (int) nm.getSize());
			} else {
				throw new FuseException("No Lock available.").initErrno(FuseException.ENOLCK);
			}
		}

		if (nm.isPrivate() && ((flags & CharonConstants.O_ACCMODE) == O_WRONLY || (flags & CharonConstants.O_ACCMODE) == O_RDWR)) {
			lockedFiles.put(nm.getIdpath(), (int) nm.getSize());
		}

		openSetter.setFh(nm);

		return 0;
	}

	@Override
	public int read(String path, Object fh, ByteBuffer buf, long offset) throws FuseException {
		Statistics.execOp(Statistics.READ);

		if (lastReadPath != null && !lastReadPath.equals(path)) {
			Printer.println("\n [" + numRead + "] x :::: READ( " + lastReadPath + ", offset:" + offset + " )", "amarelo");
			numRead = 0;
		} else {
			numRead++;
		}
		lastReadPath = path;

		if (path.equals("/.statistics.txt")) {
			buf.put(Statistics.getReport().getBytes());
			return 0;
		}

		NodeMetadata metadata;
		metadata = (NodeMetadata) fh;

		if (!metadata.isDirectory() && !metadata.isPending()) {
			int res = daS.readData(metadata.getIdpath(), buf, (int) offset, buf.capacity(), metadata.getDataHashList(), metadata.isPending(), metadata.getSize(), metadata.getLocation(), metadata.getExternalManaged());
			if (res != 0) {
				throw new FuseException("Cannot read.").initErrno(FuseException.EIO);
			}
		}
		return 0;
	}

	@Override
	public int write(String path, Object fh, boolean isWritepage, ByteBuffer buf, long offset) throws FuseException {
		Statistics.execOp(Statistics.WRITE);

		if (lastWritePath != null && !lastWritePath.equals(path)) {
			Printer.println("\n [" + numWrite + "] x :::: WRITE( " + lastWritePath + "offset: " + offset + " )", "amarelo");
			numWrite = 0;
		} else {
			numWrite++;
		}
		lastWritePath = path;

		if (path.equals("/.statistics.txt")) {
			return 0;
		}

		NodeMetadata metadata = (NodeMetadata) fh;
		if (fh == null || metadata.isExternalManaged()) {
			throw new FuseException("Cannot delete directories inside .external directory.").initErrno(FuseException.EOPNOTSUPP);
		}

		long size = daS.writeData(metadata.getIdpath(), buf, (int) offset, metadata.isPending(), metadata.getSize(), metadata.getDataHashList(), metadata.getLocation());

		if (size == -1) {
			throw new FuseException("IOException on write.").initErrno(FuseException.EIO);
		}

		metadata.setSize(size);
		metadata.setPending(false);

		return 0;
	}

	@Override
	public int rename(String from, String to) throws FuseException {
		printRW();
		Statistics.execOp(Statistics.RENAME);

		Printer.println("\n :::: RENAME( from: " + from + ", to: " + to + " )", "amarelo");
		if (from.equals("/.statistics.txt")) {
			return 0;
		}

		//TODO: ver se é possivel fazer o LINK funcionar com gedit.
		//TODO: mover files entre NameSpaces.
		String[] vec = dividePath(to);

		//access control
		NameSpace ns = directoryService.getNS(from);

		accessControl(ns);

		long time = System.currentTimeMillis();
		NodeMetadata metadata = ns.getMetadata(from);
		Statistics.getMeta(System.currentTimeMillis() - time);

		if (metadata.isExternalManaged()) {
			throw new FuseException("Cannot delete directories inside .external directory.").initErrno(FuseException.EOPNOTSUPP);
		}

		lock(ns);

		metadata.setParent(vec[0]);
		metadata.setName(vec[1]);

		time = System.currentTimeMillis();
		directoryService.updateMetadata(from, metadata, ns);
		Statistics.updateMeta(System.currentTimeMillis() - time);

		return 0;
	}

	@Override
	public int chmod(String path, int mode) throws FuseException {
		printRW();
		Statistics.execOp(Statistics.CHMOD);

		Printer.println("\n :::: CHMOD( " + path + ", mode: " + mode + " )", "amarelo");
		if (path.equals("/.statistics.txt")) {
			return 0;
		}

		long time = System.currentTimeMillis();
		NameSpace ns = directoryService.getNS(path);

		NodeMetadata metadata = ns.getMetadata(path);
		Statistics.getMeta(System.currentTimeMillis() - time);

		if (metadata.isExternalManaged()) {
			return 0;
		}

		if (mode == metadata.getMode()) {
			return 0;
		}

		accessControl(ns);

		lock(ns);

		metadata.setMode(mode);

		time = System.currentTimeMillis();
		directoryService.updateMetadata(path, metadata, ns);
		Statistics.updateMeta(System.currentTimeMillis() - time);

		return 0;
	}

	@Override
	public int flush(String path, Object fh) throws FuseException {
		printRW();
		Statistics.execOp(Statistics.FLUSH);

		Printer.println("\n :::: FLUSH( " + path + " )", "amarelo");

		if (path.equals("/.statistics.txt")) {
			return 0;
		}

		NodeMetadata mdata = (NodeMetadata) fh;
		if (mdata != null && !mdata.isPending()) {
			if (lockedFiles.containsKey(mdata.getIdpath()) && lockedFiles.get(mdata.getIdpath()) != (int) mdata.getSize()) {
				daS.syncWClouds(mdata.getIdpath(), mdata.getLocation());
			}
		}

		return 0;
	}

	@Override
	public int fsync(String path, Object fh, boolean isDatasync) throws FuseException {
		printRW();
		Statistics.execOp(Statistics.FSYNC);

		Printer.println("\n :::: FSYNC(" + path + ", isDatasync: " + isDatasync + " )", "amarelo");
		if (path.equals("/.statistics.txt")) {
			return 0;
		}

		NodeMetadata nm = (NodeMetadata) fh;
		if (!config.isFsyncToCloud()) {
			daS.syncWDisk(nm.getIdpath(), nm.getLocation());
		} else {
			daS.syncWClouds(nm.getIdpath(), nm.getLocation());
		}

		return 0;
	}

	@Override
	public int release(String path, Object fh, int flags) throws FuseException {
		printRW();
		Statistics.execOp(Statistics.RELEASE);

		Printer.println("\n :::: RELEASE ( " + path + ", flags: " + flags + " )", "amarelo");

		if (path.equals("/.statistics.txt")) {
			return 0;
		}

		NodeMetadata nm = ((NodeMetadata) fh);
		if (nm.isExternalManaged()) {
			return 0;
		}

		daS.cleanMemory(nm.getIdpath()); //para eliminar o ficheiro da memória

		if (lockedFiles.containsKey(nm.getIdpath()) && (flags & CharonConstants.O_ACCMODE) == O_WRONLY || (flags & CharonConstants.O_ACCMODE) == O_RDWR) {
			daS.releaseData(nm.getIdpath(), nm.isPending()); //para tirar o lock
			lockedFiles.remove(nm.getIdpath());
		}

		return 0;
	}

	@Override
	public int truncate(String path, long size) throws FuseException {
		printRW();
		Statistics.execOp(Statistics.TRUNCATE);

		Printer.println("\n :::: TRUNCATE(" + path + ", size:" + size + " )", "amarelo");

		if (path.equals("/.statistics.txt")) {
			return 0;
		}

		NameSpace ns = directoryService.getNS(path);
		long time = System.currentTimeMillis();
		NodeMetadata metadata = ns.getMetadata(path);
		if (metadata.isExternalManaged()) {
			return 0;
		}

		Statistics.getMeta(System.currentTimeMillis() - time);

		//access control
		accessControl(ns);

		lock(ns);

		metadata.setBlocks((int) ((size + 511L) / 512L));
		metadata.setSize(size);

		daS.truncData(metadata.getPath(), metadata.getIdpath(), (int) size, metadata.getDataHashList(), false, metadata.isPending(), metadata.getSize(), metadata.getLocation());
		time = System.currentTimeMillis();
		Statistics.updateMeta(System.currentTimeMillis() - time);

		return 0;
	}

	@Override
	public int rmdir(String path) throws FuseException {
		printRW();
		Statistics.execOp(Statistics.RMDIR);

		Printer.println("\n :::: RMDIR(" + path + " )", "amarelo");

		NameSpace ns = directoryService.getNS(path);
		NodeMetadata m = ns.getMetadata(path);
		if (m.isExternalManaged()) {
			throw new FuseException("Cannot delete directories inside .external directory.").initErrno(FuseException.EOPNOTSUPP);
		}

		//access control
		accessControl(ns);

		lock(ns);

		long time = System.currentTimeMillis();
		directoryService.removeMetadata(path, ns);
		Statistics.delMeta(System.currentTimeMillis() - time);

		return 0;
	}

	@Override
	public int statfs(FuseStatfsSetter statfsSetter) throws FuseException {
		printRW();
		Statistics.execOp(Statistics.STATFS);

		Printer.println("\n :::: STATFS()", "amarelo");
		statfsSetter.set(statfs.blockSize, statfs.blocks, statfs.blocksFree, statfs.blocksAvail, statfs.files, statfs.filesFree, statfs.namelen);
		return 0;
	}

	@Override
	public int unlink(String path) throws FuseException {
		printRW();
		Statistics.execOp(Statistics.UNLINK);
		Printer.println("\n :::: UNLINK( " + path + " )", "amarelo");

		if (path.equals("/.statistics.txt")) {
			throw new FuseException("Cannot delete .statistics.txt file.").initErrno(FuseException.EOPNOTSUPP);
		}

		long time = System.currentTimeMillis();
		NameSpace ns = directoryService.getNS(path);
		Statistics.getMeta(System.currentTimeMillis() - time);

		NodeMetadata m = ns.getMetadata(path);
		if (m.isExternalManaged()) {
			throw new FuseException("Cannot delete directories inside .external directory.").initErrno(FuseException.EOPNOTSUPP);
		}

		//access control
		accessControl(ns);;

		lock(ns);

		NodeMetadata metadata = ns.getMetadata(path);

		daS.deleteData(metadata.getIdpath(), metadata.getLocation());

		time = System.currentTimeMillis();
		directoryService.removeMetadata(path, ns);
		Statistics.delMeta(System.currentTimeMillis() - time);

		return 0;
	}

	@Override
	public int utime(String path, int atime, int mtime) throws FuseException {
		printRW();
		Statistics.execOp(Statistics.UTIME);

		if (path.equals("/.statistics.txt")) {
			return 0;
		}

		long time = System.currentTimeMillis();
		NameSpace ns = directoryService.getNS(path);
		NodeMetadata metadata = ns.getMetadata(path);
		Statistics.getMeta(System.currentTimeMillis() - time);

		if (metadata.isExternalManaged()) {
			return 0;
		}

		accessControl(ns);
		lock(ns);

		metadata.setAtime(atime);
		metadata.setMtime(mtime);

		time = System.currentTimeMillis();
		directoryService.updateMetadata(path, metadata, ns);
		Statistics.updateMeta(System.currentTimeMillis() - time);

		return 0;
	}

	@Override
	public int chown(String path, int uid, int gid) throws FuseException {
		printRW();
		Statistics.execOp(Statistics.CHOWN);

		Printer.println("\n :::: CHOWN(" + path + ", uid: " + uid + ", gid:" + gid + " )", "amarelo");

		if (path.equals("/.statistics.txt")) {
			return 0;
		}

		long time = System.currentTimeMillis();
		NameSpace ns = directoryService.getNS(path);
		NodeMetadata metadata = ns.getMetadata(path);
		Statistics.getMeta(System.currentTimeMillis() - time);

		if (metadata.isExternalManaged()) {
			return 0;
		}

		accessControl(ns);
		lock(ns);

		metadata.setUid(uid);
		metadata.setGid(gid);

		time = System.currentTimeMillis();
		directoryService.updateMetadata(path, metadata, ns);
		Statistics.updateMeta(System.currentTimeMillis() - time);

		return 0;
	}

	@Override
	public int link(String from, String to) throws FuseException {
		printRW();
		Statistics.execOp(Statistics.LINK);

		Printer.println("\n :::: LINK(from: " + from + ", to:" + to + " )", "amarelo");

		if (from.equals("/.statistics.txt")) {
			return 0;
		}

		long time = System.currentTimeMillis();
		NameSpace ns = directoryService.getNS(from);
		NodeMetadata fromMeta = ns.getMetadata(from);

		if (fromMeta.isExternalManaged()) {
			return 0;
		}

		lock(ns);

		Statistics.getMeta(System.currentTimeMillis() - time);
		String[] vec = dividePath(to);
		int nlink = fromMeta.getNlink() + 1;
		fromMeta.setNlink(nlink);

		NodeMetadata m = fromMeta;
		try {
			m = (NodeMetadata) fromMeta.clone();
			m.setParent(vec[0]);
			m.setName(vec[1]);

			time = System.currentTimeMillis();
			directoryService.putMetadata(m, ns);
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}

		Statistics.putMeta(System.currentTimeMillis() - time);
		if (nlink > 2) {
			time = System.currentTimeMillis();
			Collection<NodeMetadata> l = directoryService.getAllLinks(fromMeta.getIdpath());
			for (NodeMetadata meta : l) {
				meta.setNlink(nlink);
				time = System.currentTimeMillis();
				directoryService.updateMetadata(meta.getPath(), meta, ns);
				Statistics.updateMeta(System.currentTimeMillis() - time);
			}
		}

		return 0;
	}

	@Override
	public int symlink(String from, String to) throws FuseException {
		printRW();
		Statistics.execOp(Statistics.SYMLINK);

		Printer.println("\n :::: SYMLINK(from: " + from + ", to:" + to + " )", "ciao");

		if (from.equals("/.statistics.txt")) {
			return 0;
		}

		NameSpace ns = directoryService.getNS(from);
		NodeMetadata meta = ns.getMetadata(from);
		if (meta.isExternalManaged()) {
			throw new FuseException("Cannot delete directories inside .external directory.").initErrno(FuseException.EOPNOTSUPP);
		}

		lock(ns);

		String[] vec = dividePath(to);

		NodeMetadata m = NodeMetadata.getDefaultNodeMetadata(vec[0], vec[1], NodeType.SYMLINK, 0, getNextIdPath(ns), ns.getMetadata(from).getLocation());

		directoryService.putMetadata(m, ns);

		return 0;
	}

	@Override
	public int readlink(String path, CharBuffer link) throws FuseException {
		printRW();
		Statistics.execOp(Statistics.READLINK);
		Printer.println("\n :::: READLINK(" + path + ", link: " + link + " )", "amarelo");

		NodeMetadata nm = null;
		try {
			long time = System.currentTimeMillis();
			nm = directoryService.getMetadata(path);
			Statistics.getMeta(System.currentTimeMillis() - time);
		} catch (DirectoryServiceException e) {
			throw new FuseException(e.getMessage()).initErrno(FuseException.ECONNABORTED);
		}
		link.put(nm.getIdpath());

		return 0;
	}

	public int getxattr(String path, String name, ByteBuffer buf) throws FuseException, BufferOverflowException {
		printRW();
		Printer.println("\n :::: GETXATTR(" + path + ", name: " + name + " )", "amarelo");

		if (path.equals("/.statistics.txt")) {
			return 0;
		}
		
		NameSpace ns = directoryService.getNS(path);
		if (ns == null) {
			throw new FuseException("no such node.").initErrno(FuseException.ENOENT);
		}

		NodeMetadata m = ns.getMetadata(path);

		if (m.isExternalManaged()) {
			return 0;
		}

		byte[] array = m.getXattr().get(name);
		if (array != null) {
			buf.put(array);
		}

		return 0;
	}

	@Override
	public int getxattrsize(String path, String name, FuseSizeSetter setter)
			throws FuseException {
		printRW();
		Printer.println("\n :::: GETXATTRSIZE(" + path + ", name: " + name + " )", "amarelo");

		
		if (path.equals("/.statistics.txt")) {
			setter.setSize(0);
			return 0;
		}
		
		NameSpace ns = directoryService.getNS(path);
		if (ns == null) {
			throw new FuseException("no such node.").initErrno(FuseException.ENOENT);
		}

		NodeMetadata m = ns.getMetadata(path);

		byte[] array = m.getXattr().get(name);
		if (array != null) {
			setter.setSize(array.length);
		}

		return 0;
	}

	@Override
	public int listxattr(String path, XattrLister list) throws FuseException {
		printRW();
		Printer.println("\n :::: LISTXATTR(" + path + " )", "amarelo");

		if (path.equals("/.statistics.txt")) {
			return 0;
		}
		
		NameSpace ns = directoryService.getNS(path);
		if (ns == null) {
			throw new FuseException("no such node.").initErrno(FuseException.ENOENT);
		}

		NodeMetadata m = ns.getMetadata(path);

		//		if(m.isExternalManaged())
		//			return 0;
		for (String name : m.getXattr().keySet()) {
			list.add(name);
		}

		return 0;
	}

	@Override
	public int removexattr(String path, String name) throws FuseException {
		printRW();
		Printer.println("\n :::: REMOVEXATTR(" + path + ", name: " + name + " )", "amarelo");

		NameSpace ns = directoryService.getNS(path);
		if (ns == null) {
			throw new FuseException("no such node.").initErrno(FuseException.ENOENT);
		}

		NodeMetadata m = ns.getMetadata(path);

		//access control
		accessControl(ns);

		lock(ns);

		byte[] array = m.getXattr().remove(name);
		if (array != null) {
			directoryService.updateMetadata(path, m, ns);
		}

		return 0;
	}

	@Override
	public int setxattr(String path, String name, ByteBuffer value, int flags)
			throws FuseException {

		printRW();
		try {
			value.order(ByteOrder.LITTLE_ENDIAN);
			byte[] buff = new byte[value.remaining()];
			value.get(buff);

			value.rewind();
			value.getInt();
			value.getInt();
			value.getInt();
			value.getShort();
			int perm = value.getShort();
			int id = value.getInt();

			Printer.println("\n :::: SETXATTR(" + path + ", name: " + name + ", " + id + ":" + perm + " )", "amarelo");

			//			if(directoryService.getEmail(id) == null)
			//				throw new FuseException("Email not found. (client " + id + ")").initErrno(FuseException.ENOTSUPP);
			NameSpace ns = directoryService.getNS(path);

			NodeMetadata m = ns.getMetadata(path);

			if (!m.isExternalManaged() && (!m.isDirectory() || !directoryService.isFolderEmpty(path))) {
				throw new FuseException("not Supported operation").initErrno(FuseException.ENOTSUPP);
			}

			//access control
			accessControl(ns);

			if (!ns.getSnsInfo().isNSPrivate() && !m.getIdpath().equals(ns.getId())) {
				throw new FuseException("not Supported operation").initErrno(FuseException.ENOTSUPP);
			}

			//TODO: remove and add peers in an already shared folder.
			lock(ns);

			m.getXattr().put(name, buff);

			//read the cannonical ids from remote peer
			LinkedList<Pair<String, String[]>> cannonicalIds = new LinkedList<Pair<String, String[]>>();
			RemoteLocationEntry peer = directoryService.getRemotePeer(id);
			if (peer == null) {
				throw new FuseException("setPermition error.").initErrno(FuseException.ECONNABORTED);
			}

			String idPath = m.getIdpath();
			if (idPath.contains("#")) {
				idPath = idPath.split("#", 2)[1];
			}
			String snsName = getSNSNameFromIdPATH(idPath);
			if (!peer.isRemoteButUseTheSameAccounts()) {
				cannonicalIds = peer.getCannonicalIds();

				List<Pair<String, String[]>> credsToShare = new LinkedList<Pair<String, String[]>>();
				if (perm != 1) {
					String permissions = null;
					if (perm == 4 || perm == 5) {
						permissions = "r";
					} else if (perm == 2 || perm == 3) {
						permissions = "w";
					} else if (perm == 0) {
						System.out.println("NO PERM - (u:id:---)");
					} else {
						permissions = "rw";
					}

					credsToShare = daS.setPermission(snsName, permissions, cannonicalIds, m.getLocation());
					if (credsToShare == null) {
						throw new FuseException("setPermition error.").initErrno(FuseException.ECONNABORTED);
					}

//					System.out.println();
//
//					System.out.println("-> before add miss creds");
//					for (Pair<String, String[]> asd : credsToShare) {
//						for (String str : asd.getValue()) {
//							System.out.println(asd.getKey() + " - " + str);
//						}
//					}
					addMissingCredentials(credsToShare);
//					System.out.println("-> after add miss creds");
//					for (Pair<String, String[]> asd : credsToShare) {
//						for (String str : asd.getValue()) {
//							System.out.println(asd.getKey() + " - " + str);
//						}
//					}

					NSAccessInfo sharedInfo = new NSAccessInfo(credsToShare, clientId, m.getLocation());
					sharedInfo.addPeer(id, permissions);
					
					if (m.isPrivate()) {
						privateToPublic(path, snsName, m, id, sharedInfo);
					}
					String[] pathSplited = path.split("/");
					sendNewNSs(snsName, sharedInfo, pathSplited[pathSplited.length - 1]);
				} else {
					System.out.println("NO PERM - (u:id:x)");
				}
			} else {
				//if the owner is sharing with a Charon instance that use the same api credentials
				NSAccessInfo sharedInfo = new NSAccessInfo(null, clientId, m.getLocation());
				sharedInfo.addPeer(id, "rw");
				if (m.isPrivate()) {
					privateToPublic(path, snsName, m, id, sharedInfo);
				}
				String[] pathSplited = path.split("/");
				sendNewNSs(snsName, sharedInfo, pathSplited[pathSplited.length - 1]);
			}
		} catch (DirectoryServiceException e) {
			e.printStackTrace();
			throw new FuseException(e.getMessage()).initErrno(FuseException.ENOENT);
		}
		return 0;
	}

	private void printFileId(NodeMetadata m) {
		String filename = m.getName();
		String pathId = m.getIdpath();
		String asteriskColor = Printer.ANSI_RED;
		String letterColor = Printer.ANSI_WHITE;

		String asterisk = "\t" + asteriskColor + "*******";
		for (int i = 0; i < (filename.length() + pathId.length()); i++) {
			asterisk = asterisk.concat("*");
		}
		asterisk = asterisk.concat("\u001B[0m");

		String result = "\n" + asterisk + "\n" + "\t" + asteriskColor + "* " + letterColor + filename + " = " + pathId + asteriskColor + " *" + "\n" + asterisk + "\n";

		System.out.println(result);
	}

	private void accessControl(NameSpace ns) throws FuseException {
		String perm = ns.getSnsInfo().getPermissions(config.getClientId());
		if (perm == null || !perm.contains("w")) {
			throw new FuseException("Permission denied.").initErrno(FuseException.EPERM);
		}
	}

	private void lock(NameSpace ns) throws FuseException {
		if (!directoryService.isPrivate(ns) && !lockService.justLock(ns.getId(), LockService.LOCK_TIME)) {
			throw new FuseException("NO LOCKS AVAILABLE.").initErrno(FuseException.ENOLCK);
		}
	}

	public static String getNextIdPath(NameSpace ns) {
		return ns.getId() + "#" + clientId + (System.currentTimeMillis() - 1449064221733L) + (fileNum % 500);

	}

	private void addMissingCredentials(List<Pair<String, String[]>> credsToShare) throws FuseException {
		try {
			//			String [] split;
			List<String[]> missing = RemoteLocationEntry.readCannonicalIds(clientId, new File(CharonConstants.CREDENTIALS_FILE_NAME));

			for (String[] s : missing) {
				//				if(s[0].equals("email")){
				//					split = s[1].split("@");
				//					if(split[1].equals("gmail.com")){
				//						for(Pair<String, String[]> p : credsToShare){
				//							if(p.getKey().equals("GOOGLE-STORAGE")){
				//								String [] newAux = Arrays.copyOf(p.getValue(), p.getValue().length+1);
				//								newAux[newAux.length-1]=s[1];
				//								p.setValue(newAux);
				//							}
				//
				//						}
				//					}
				//				}else{
				for (Pair<String, String[]> p : credsToShare) {
					if (p.getKey().equals(s[0])) {
						String[] newAux = Arrays.copyOf(p.getValue(), p.getValue().length + 1);
						newAux[newAux.length - 1] = s[1];
						p.setValue(newAux);
					} else if (p.getKey().equals("GOOGLE-STORAGE") && s[0].equals("email")) {
						String[] newAux = Arrays.copyOf(p.getValue(), p.getValue().length + 1);
						newAux[newAux.length - 1] = s[1];
						p.setValue(newAux);
					}
				}
			}
			//			}

		} catch (ParseException e) {
			throw new FuseException("bad  " + CharonConstants.CREDENTIALS_FILE_NAME + " file").initErrno(FuseException.ELIBBAD);
		}
	}

	private void sendNewNSs(String nsId, NSAccessInfo accInfo, String folder) {
		File f = new File("share_" + nsId + ".charon");
		if (!f.exists()) {
			try {
				while (!f.createNewFile());
			} catch (IOException e) {
			}
		}

		try {
			JSONObject json = accInfo.toJson();
			json.put("NS-id", nsId);
			FileUtils.writeStringToFile(f, json.toString());

		} catch (JSONException ex) {
			ex.printStackTrace();
		} catch (IOException ex) {
			ex.printStackTrace();
		}

		//        try {
		//            FileOutputStream fos = new FileOutputStream(f);
		//            ObjectOutputStream ois = new ObjectOutputStream(fos);
		//            ois.writeUTF(nsId);
		//            ois.writeObject(accInfo);
		//
		//            ois.close();
		//            fos.close();
		//
		//        } catch (FileNotFoundException e) {
		//            e.printStackTrace();
		//        } catch (IOException e) {
		//            e.printStackTrace();
		//        }

	}

	private void privateToPublic(String path, String snsName, NodeMetadata m, int id, NSAccessInfo sharedInfo) throws DirectoryServiceException {
		m.setIsPrivate(false);

		directoryService.share(path, snsName, id, sharedInfo);
	}

	private String getSNSNameFromIdPATH(String idpath){
		if(!idpath.startsWith("charon-"))
			return "charon-" + clientId + "-sns-"+idpath;
		else
			return idpath;
	}

	private String[] dividePath(String path) {
		if (path.equals("/")) {
			return new String[]{"root", ""};
		}

		String[] toRet = new String[2];
		String[] split = path.split("/");
		toRet[1] = split[split.length - 1];
		if (split.length == 2) {
			toRet[0] = path.substring(0, path.length() - toRet[1].length());
		} else {
			toRet[0] = path.substring(0, path.length() - toRet[1].length() - 1);
		}
		return toRet;
	}

	private void printRW() {
		if (lastReadPath != null) {
			Printer.println("\n [" + numRead + "] x :::: READ( " + lastReadPath + " )", "amarelo");
		}

		if (lastWritePath != null) {
			Printer.println("\n [" + numWrite + "] x :::: WRITE( " + lastWritePath + " )", "amarelo");
		}

		lastReadPath = null;
		lastWritePath = null;
		numRead = 0;
		numWrite = 0;
	}

	public static void main(String[] args) {

		Log l = LogFactory.getLog(Charon.class);

		String[] fuseArgs = new String[9];
		String[] charonArgs = new String[args.length - 2];
		System.arraycopy(args, 0, fuseArgs, 0, fuseArgs.length - 7);
		System.arraycopy(args, 2, charonArgs, 0, charonArgs.length);

		String charonConfigFileName = CharonConstants.CHARON_CONFIG_FILE_NAME;
		String locationsConfigFileName = CharonConstants.LOCATIONS_CONFIG_FILE_NAME;
		String[] addSNS = null;
		for (int i = 0; i < charonArgs.length; i++) {
			if (charonArgs[i].startsWith(CharonConstants.TAG_NS_NAMES)) {
				try {
					addSNS = charonArgs[i].split("=")[1].split(",");
				} catch (Exception e) {
					System.out.println("(-) Invalid Charon Arguments.");
					System.exit(0);
				}
			} else if (charonArgs[i].startsWith(CharonConstants.TAG_CHARON_CONFIG_FILE)) {
				try {
					charonConfigFileName = charonArgs[i].split("=")[1];
				} catch (Exception e) {
					System.out.println("(-) Invalid Charon Arguments.");
					System.exit(0);
				}
			} else if (charonArgs[i].startsWith(CharonConstants.TAG_LOCATIONS_FILE)) {
				try {
					locationsConfigFileName = charonArgs[i].split("=")[1];
				} catch (Exception e) {
					System.out.println("(-) Invalid Charon Arguments.");
					System.exit(0);
				}
			} else {
				System.out.println("(-) Invalid Charon Arguments. [ " + charonArgs[i] + " ]");
				System.exit(0);
			}
		}

		try {
			File f = new File(charonConfigFileName);
			Properties props = new Properties();
			FileInputStream fis = new FileInputStream(f);
			props.load(fis);
			fis.close();
			try {
				CharonConfiguration config = new CharonConfiguration(props, locationsConfigFileName, charonConfigFileName);
				//				System.out.println(" ============================== ");
				//				System.out.println(config.toString());
				//				System.out.println(" ============================== ");
				f = new File(config.getMountPoint());
				if (!f.exists()) {
					while (!f.mkdirs());
				}


				// Ctrl^C handle
				Runtime.getRuntime().addShutdownHook(new ShutDownThread(config.getMountPoint(), Thread.currentThread()));
				fuseArgs[2] = config.getMountPoint();
				fuseArgs[3] = "-o";
				fuseArgs[4] = "uid=" + System.getProperty("uid");
				fuseArgs[5] = "-o";
				fuseArgs[6] = "gid=" + System.getProperty("gid");
				fuseArgs[7] = "-o";
				fuseArgs[8] = "allow_other";
				FuseMount.mount(fuseArgs, new Charon(config, addSNS), l);
			} catch (ParseException e1) {
				System.out.println("(-) " + e1.getMessage());
			} catch (Exception e) {
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			System.out.println("(-) Error while reading " + charonConfigFileName + ". File not found.");
		} catch (IOException e2) {
			System.out.println("(-) Error while reading " + charonConfigFileName + ".");
		}

	}

}
