package charon.directoryService;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;

import charon.configuration.CharonConfiguration;
import charon.configuration.Location;
import charon.configuration.storage.remote.RemoteConfiguration;
import charon.configuration.storage.remote.RemoteLocationEntry;
import charon.directoryService.exceptions.DirectoryServiceException;
import charon.general.LockUpdterNSSwitcher;
import charon.general.NSAccessInfo;
import charon.general.Printer;
import charon.storageService.StorageService;
import charon.storageService.accessors.AmazonAcessor;
import charon.storageService.accessors.DepSkyAcessor;
import charon.storageService.accessors.IMetadataAccessor;
import charon.util.IOUtil;
import depsky.client.messages.metadata.ExternalMetadata;
import depsky.util.Pair;
import depsky.util.integrity.IntegrityManager;
import fuse.FuseDirFiller;

//TODO: Suporte a HARD LINKS e SYMLINKS!
public class DirectoryServiceImpl {

    private static final long NS_UPDATER_DELTA = 2000;
    private ConcurrentHashMap<String, NameSpace> nsBag;
//    private StorageService storageService;
    private String pathId;
    private int clientid;
//    private ConcurrentHashMap<String, FileVersionsManager> versionManager;
    private ConcurrentHashMap<String, NSUpdaterTimerTask> nsUpdaterBag;

    private Timer timer;
    private IMetadataAccessor accessor;
    private LockUpdterNSSwitcher lockSwitcher;

    public DirectoryServiceImpl(CharonConfiguration config, int clientId, LockUpdterNSSwitcher lockService) throws IOException {
        this.nsBag = new ConcurrentHashMap<String, NameSpace>();
        this.pathId = config.getPnsId();
        this.clientid = clientId;
//        this.versionManager = new ConcurrentHashMap<String, FileVersionsManager>();
        this.lockSwitcher = lockService;

        switch (config.getDefaultLocation()) {
            case CoC:
                this.accessor = DepSkyAcessor.getDirectInstance(clientId, config, config.getCoCConfiguration(), this);
                break;
            case SINGLE_CLOUD:
                this.accessor = new AmazonAcessor(clientId, config.getSingleCloudConfig().getAccessKey(), config.getSingleCloudConfig().getSecretKey(), config, this);
                break;
            default:
                System.out.print("Unsuported default location to store metadata.");
                if (config.getCoCConfiguration() != null) {
                    System.out.println(" CoC will be used.");
                    this.accessor = DepSkyAcessor.getDirectInstance(clientId, config, config.getCoCConfiguration(), this);
                    break;
                } else if (config.getSingleCloudConfig() != null){
                     System.out.println(" Single cloud will be used.");
                     this.accessor = new AmazonAcessor(clientId, config.getSingleCloudConfig().getAccessKey(), config.getSingleCloudConfig().getSecretKey(), config, this);
                     break;
                }else{
                    System.out.println(" ABORTING!");
                    System.exit(0);
                }
                 break;
        }

//        this.accessor = DepSkyAcessor.getDirectInstance(clientId, config, config.getCoCConfiguration(), this);
        this.lockSwitcher.initNSUpdater(this, accessor);
        this.nsUpdaterBag = new ConcurrentHashMap<String, NSUpdaterTimerTask>();
        this.timer = new Timer();
        initBag(config);
    }

    public NodeMetadata getMetadata(String path) throws DirectoryServiceException {
        NodeMetadata m = null;
        for (NameSpace ns : nsBag.values()) {
            m = ns.getMetadata(path);
            if (m == null) {
                continue;
            }
            //			m.setNSPathId(ns.getId());
//			if(!isPrivate(ns) && m.isFile() && !lockSwitcher.isLocked(ns.getId())){
//				FileHashRepresentation hashRep = versionManager.get(ns.getId()).getFileHashRep(m.getIdpath());
//				if(hashRep!=null){
//					m.setDataHashMap(hashRep.getHashes());
//					m.setSize(hashRep.getFileSize());
//				}
//			}
            return m;
        }
        throw new DirectoryServiceException("Node not exists");
    }

    public void putMetadata(NodeMetadata m, NameSpace ns) {

        m.setIsPrivate(isPrivate(ns));
        m.setNSPathId(ns.getId());
        ns.putMetadata(m);
//		if(!ns.putChildren(m)){
//			NameSpace parentNs = getNS(m.getParent());
//			DirectoryNodeMetadata parent = (DirectoryNodeMetadata)parentNs.getMetadata(m.getParent());
//			parent.putChild(m);
//		}

        if (ns.getId().equals(pathId)) {
            writeMetadata();
        } else {
            writeNameSpace(ns, false, false);
        }

    }

    public NameSpace getNS(String path) {
        NameSpace res = nsBag.get(pathId);
        if (res.containsIdPath(path)) {
            return res;
        }

        for (NameSpace ns : nsBag.values()) {
            if (ns.containsMetadata(path)) {
                return ns;
            }
        }
        return null;
    }

    public NodeMetadata getMetadataByPathId(String pathId) {
        for (NameSpace ns : nsBag.values()) {
            NodeMetadata res = ns.getMetadataFromIdPath(pathId);
            if (res != null) {
                return res;
            }
        }
        return null;
    }

    public void removeMetadata(String path, NameSpace ns) {
        NodeMetadata m = ns.removeMetadata(path);

//		if(!ns.removeChild(m.getParent(), path)){
//			NameSpace parentNs = getNS(m.getParent());
//			DirectoryNodeMetadata parent = (DirectoryNodeMetadata) parentNs.getMetadata(m.getParent());
//			parent.removeChild(path);
//		}
        if (ns.getId().equals(pathId)) {
            writeMetadata();
        } else {
            writeNameSpace(ns, true, false);
            if (ns.getNumberOfNodes() == 0) {
                nsBag.remove(ns.getId());
                lockSwitcher.release(ns.getId());
                writeMetadata();
            }
        }
    }

    public void getNodeChildren(String path, FuseDirFiller dirFiller) {
//		NameSpace ns = getNS(path);
//		DirectoryNodeMetadata metadata = (DirectoryNodeMetadata) ns.getMetadata(path);

//		for(NodeMetadata m : metadata.getChildren())
//			dirFiller.add(m.getName(), m.getInode(), (int) m.getMode());
        for (NameSpace ns : nsBag.values()) {
            ns.getNodeChildren(path, dirFiller);
        }

    }

    public boolean isFolderEmpty(String path) {
        for (NameSpace ns : nsBag.values()) {
            if (!ns.isFolderEmpty(path)) {
                return false;
            }
        }
        return true;
    }

    public void updateMetadata(String path, NodeMetadata m, NameSpace ns) {
        ns.updateMetadata(path, m);
        if (ns.getId().equals(pathId)) {
            writeMetadata();
        } else {
            writeNameSpace(ns, false, false);
        }
    }

    public Collection<NodeMetadata> getAllLinks(String idPath) {
        Collection<NodeMetadata> res = new LinkedList<NodeMetadata>();
        for (NameSpace ns : nsBag.values()) {
            res.addAll(ns.getAllLinks(idPath));
        }

        return res;
    }

    public void insertMetadataInBuffer(String path, NodeMetadata metadata) throws DirectoryServiceException {
        NameSpace ns = getNS(path);
        if (ns != null) {
            ns.insertMetadataInBuffer(path, metadata);
        }
    }

    public void commitMetadataBuffer(String idPath, ExternalMetadata hash, boolean lastBlock) throws DirectoryServiceException {

        if (!isNS(idPath)) {
            String[] splitIdPath = idPath.split("_");
            NameSpace ns = getNSbyIdPath(splitIdPath[0]);
            if (ns != null) {
                ns.commitMetadataBuffer(splitIdPath[0], hash, Integer.parseInt(splitIdPath[1]) - 1);

                if (ns.getId().equals(pathId)) {
                    if (lastBlock) {
                        writeMetadata();
                    }
                } else if (lastBlock) {
                    writeNameSpace(ns, true, false);
                }
            }
        }
    }

    public boolean isPrivate(NameSpace ns) {
        return ns.getId().equals(pathId);
    }

//    public void setStorageService(StorageService daS) throws IOException {
//        this.storageService = daS;
//        initBag();
//    }
    public void addNS(String idPath, NSAccessInfo accInfo) {
        try {
            NameSpace ns = readNS(idPath + "#" + idPath, accInfo);
            if (ns != null && !nsBag.containsKey(idPath)) {
                nsBag.put(idPath, ns);
//                versionManager.put(idPath, new FileVersionsManager(idPath, depskyAccessor));
                lockSwitcher.addNameSpaceToManage(ns);
                writeMetadata();
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println(e.getLocalizedMessage() + " : DiS.addNS.");
        }
    }

    public void updateNS(NameSpace ns) {
        if (ns.getNumberOfNodes() == 0) {
            nsBag.remove(ns.getId());
        } else {
            nsBag.put(ns.getId(), ns);
        }
    }

    public NSAccessInfo getSNSAccessInfo(String sns) {
        NameSpace info = nsBag.get(sns);
        if (info == null) {
            return null;
        }
        return info.getSnsInfo();
    }

    public void share(String path, String snsName, int id, NSAccessInfo accessInfo) {
        NodeMetadata m = nsBag.get(pathId).getMetadata(path);
        if (m == null) {
            return;
        }

        nsBag.get(pathId).removeMetadata(path);

        String idPath = m.getIdpath();
        if (idPath.contains("#")) {
            idPath = idPath.split("#", 2)[1];
        }

        
        NameSpace ns = new NameSpace(snsName, new int[]{clientid, id}, accessInfo);
        m.setIsPrivate(false);
        m.setId_path(snsName + "#" + "ROOT");
        ns.putMetadata(m);
        
        
        nsBag.put(snsName, ns);
//        versionManager.put(ns.getId(), new FileVersionsManager(ns.getId(), depskyAccessor));
        lockSwitcher.addNameSpaceToManage(ns);
        writeNameSpace(ns, false, true);
        writeMetadata();
        for (NameSpace nsLoop : nsBag.values()) {
            if (nsLoop.containsMetadata(path)) {
                //					ns.addOwners(new int[]{id});
                writeNameSpace(nsLoop, false, true);
                break;
            }
        }
    }

    private NameSpace getNSbyIdPath(String idPath) {
        for (NameSpace ns : nsBag.values()) {
            if (ns.containsIdPath(idPath)) {
                return ns;
            }
        }
        return null;
    }

    private void writeNameSpace(NameSpace ns, boolean isHashUpdate, boolean isSync) {
        ns.incVersion();

        if (isSync) {

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = null;
            try {
                oos = new ObjectOutputStream(baos);

                ns.writeExternal(oos);
                byte[] serializedNS = baos.toByteArray();

                IOUtil.closeStream(oos);
                IOUtil.closeStream(baos);

                NameSpaceRepresentation nsRepresentation = new NameSpaceRepresentation(ns.getVersion(), IntegrityManager.getHexHash(serializedNS), serializedNS);

                baos = new ByteArrayOutputStream();
                oos = new ObjectOutputStream(baos);
                nsRepresentation.writeExternal(oos);

                byte[] bagArray = baos.toByteArray();

                IOUtil.closeStream(oos);
                IOUtil.closeStream(baos);

                Printer.println("SYNC WRITE NEW NAMESPACE :", "azul");
                accessor.writeNS(ns.getId() + "#" + ns.getId(), bagArray);
//                storageService.directWrite(ns.getId() + "#" + ns.getId(), bagArray, false);
            } catch (IOException e) {
                e.printStackTrace();
            }

            return;
        }

        if (nsUpdaterBag.get(ns.getId()) == null || !nsUpdaterBag.get(ns.getId()).isScheduled()) {
            NSUpdaterTimerTask nsUp = new NSUpdaterTimerTask(accessor, ns.getId(), !ns.getId().equals(pathId));
            nsUpdaterBag.put(ns.getId(), nsUp);
            nsUp.schedule();
            nsUp.setLastUpdate(ns);
            timer.schedule(nsUp, NS_UPDATER_DELTA);
        } else {
            nsUpdaterBag.get(ns.getId()).setLastUpdate(ns);
        }

//		if(isHashUpdate){
//			writeVersionFile(ns);
//		}
    }

    private void writeMetadata() {
        try {
            List<Pair<String, NSAccessInfo>> s = new ArrayList<Pair<String, NSAccessInfo>>();
            for (NameSpace sharedNs : nsBag.values()) {
                s.add(new Pair<String, NSAccessInfo>(sharedNs.getId(), sharedNs.getSnsInfo()));
            }
            if (nsUpdaterBag.get(pathId) == null || !nsUpdaterBag.get(pathId).isScheduled()) {
                NSUpdaterTimerTask nsUp = new NSUpdaterTimerTask(accessor, pathId, false);
                nsUpdaterBag.put(pathId, nsUp);
                nsUp.schedule();
                nsUpdaterBag.get(pathId).setSNS(s);
                nsUp.setLastUpdate(nsBag.get(pathId));
                timer.schedule(nsUp, NS_UPDATER_DELTA);
            } else {
                nsUpdaterBag.get(pathId).setSNS(s);
                nsUpdaterBag.get(pathId).setLastUpdate(nsBag.get(pathId));
            }

        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage() + "writeMetadata()");
        }
    }

    //	private void cloneBag() {
    //		lastUpdate.clear();
    //		lastUpdate.putAll(nsBag);
    //	}
//	private void writeVersionFile(NameSpace ns){
//		try {
//			//				System.out.println("---- Hash file write: FILE_NAME = " + ns.getId()+"#"+ns.getId()+"versions");
//			ByteArrayOutputStream ba = new ByteArrayOutputStream();
//			ObjectOutputStream os = new ObjectOutputStream(ba);
//			ns.writeMetadataHashs(os);
//			os.flush();
//			ba.flush();
//			byte[] array = ba.toByteArray();
//			storageService.directWrite(ns.getId()+"#"+ns.getId()+"versions", array, false);
//			IOUtil.closeStream(os);
//			IOUtil.closeStream(ba);
//		} catch (IOException e) {
//			System.out.println(e.getLocalizedMessage() + ": writeVersionsFile");
//		}
//	}
    private boolean isNS(String idPath) {
        if (idPath.endsWith("versions")) {
            return true;
        }
        String s = idPath + "#" + idPath;
        for (NameSpace ns : nsBag.values()) {
            if (ns.getId().equals(s)) {
                return true;
            }
        }
        return false;
    }

    private void initBag(CharonConfiguration config) throws IOException {

        System.out.println("  Reading PNS: " + pathId + "#" + pathId);

        byte[] buf = accessor.readNS(pathId + "#" + pathId, null);

        if (buf != null) {
            System.out.println("  A PNS was found.");

            ByteArrayInputStream bais = new ByteArrayInputStream(buf);

            ObjectInputStream ois;
            try {
                ois = new ObjectInputStream(bais);
                int size = ois.readInt();
                String key;
                NSAccessInfo nsInfo;
                NameSpace ns;
                if (size > 0) {
                    System.out.println("  Reading SNSs:");
                }
                for (int i = 0; i < size - 1; i++) {
                    key = ois.readUTF();
                    nsInfo = (NSAccessInfo) ois.readObject();
                    //					System.out.println("pathID = " + key);
                    System.out.println("   - " + key);
                    ns = readNS(key.concat("#").concat(key), nsInfo);
                    if (ns != null) {
                        nsBag.put(key, ns);
//                        versionManager.put(ns.getId(), new FileVersionsManager(ns.getId(), depskyAccessor));
                    }
                }

                key = ois.readUTF();
                //				System.out.println("key = " + key);
                ns = new NameSpace();
                ns.readExternal(ois);
                nsBag.put(key, ns);

                IOUtil.closeStream(bais);
                IOUtil.closeStream(ois);

                System.out.println(nsBag.get(pathId).getRemoteConfiguration().toString());

            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                throw new IOException();
            } catch (IOException e) {
                e.printStackTrace();
                throw e;
            }

//			for(NameSpace ns : nsBag.values()){
//				for(NodeMetadata nm : ns.getAllNodes()){
//					if(!nm.getPath().equals("/") && !ns.putChildren(nm)){
//						NameSpace parentNs = getNS(nm.getParent());
//						DirectoryNodeMetadata parent = (DirectoryNodeMetadata)parentNs.getMetadata(nm.getParent());
//						parent.putChild(nm);
//					}
//				}
//			}
            for (NameSpace ns : nsBag.values()) {
                if (!isPrivate(ns)) {
                    lockSwitcher.addNameSpaceToManage(ns);
                }
            }
        } else {
            System.out.println("  There is no PNS.");
            System.out.print("  Creating a new PNS.");
            
            NameSpace myNs = new NameSpace(pathId, new int[]{clientid}, new NSAccessInfo(null, clientid, config.getDefaultLocation()));
            myNs.putMetadata(NodeMetadata.getDefaultNodeMetadata("", "/", NodeType.DIR, 100, "001", config.getDefaultLocation()));

            myNs.setRemoteConfiguration(new RemoteConfiguration());

            nsBag.put(pathId, myNs);
            writeMetadata();
            System.out.println("..Done.");
        }
    }

    private NameSpace readNS(String idPath, NSAccessInfo accInfo) throws IOException, ClassNotFoundException {
        ByteArrayInputStream temp;
        NameSpace ns = new NameSpace();

        byte[] array = accessor.readNS(idPath, accInfo);

        if (array == null) {
            return null;
        }
        temp = new ByteArrayInputStream(array);
        ObjectInputStream objOis = new ObjectInputStream(temp);

        NameSpaceRepresentation nsRep = new NameSpaceRepresentation();
        nsRep.readExternal(objOis);

        IOUtil.closeStream(temp);
        IOUtil.closeStream(objOis);

        temp = new ByteArrayInputStream(nsRep.getSerializedNS());
        objOis = new ObjectInputStream(temp);
        ns.readExternal(objOis);

        IOUtil.closeStream(temp);
        IOUtil.closeStream(objOis);

        return ns;
    }

    public Set<String> getNodeChildren(String path) {
        Set<String> res = new HashSet<String>();

        for (NameSpace ns : nsBag.values()) {
            ns.getNodeChildren(path, res);
        }

        return res;
    }

    public void addRemotePeer(RemoteLocationEntry rle) {
        nsBag.get(pathId).getRemoteConfiguration().addRemotePeer(rle);
        writeMetadata();
    }

    public RemoteLocationEntry getRemotePeer(int id) {
        return nsBag.get(pathId).getRemoteConfiguration().getPeerConfig(id);
    }

    public void putEmail(Integer id, String email) {
        nsBag.get(pathId).getRemoteConfiguration().putEmail(id, email);
        writeMetadata();
    }

}
