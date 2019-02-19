package charon.directoryService;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

import charon.general.LockUpdterNSSwitcher;
import charon.general.Printer;
import charon.storageService.accessors.DepSkyAcessor;
import charon.storageService.accessors.IMetadataAccessor;
import charon.util.IOUtil;

public class NameSpacesUpdater extends Thread {

    private final int DELTA = 5000;
    private ConcurrentHashMap<String, NameSpace> nameSpaces;
    private ConcurrentHashMap<String, String> nShashs;
    private DirectoryServiceImpl dis;
    private IMetadataAccessor accessor;
    private LockUpdterNSSwitcher lockService;
    private ConcurrentHashMap<String, Boolean> pausedSpaces;
    private String internalActualNameSpace;

    public NameSpacesUpdater(DirectoryServiceImpl dis, IMetadataAccessor accessor, LockUpdterNSSwitcher lockService) {
        this.nameSpaces = new ConcurrentHashMap<String, NameSpace>();
        this.dis = dis;
        this.accessor = accessor;
        this.lockService = lockService;
        this.pausedSpaces = new ConcurrentHashMap<>();
        this.internalActualNameSpace = new String();
        this.nShashs = new ConcurrentHashMap<String, String>();
    }

    public void addNameSpace(NameSpace ns) {
        nameSpaces.put(ns.getId(), ns);
    }

    @Override
    public void run() {

        int retry;
        boolean error;
        while (true) {
            internalActualNameSpace = new String();
            try {
                Thread.sleep(DELTA);
            } catch (InterruptedException e) {
            }
            NameSpace auxNs;
            ByteArrayInputStream bais;
            ObjectInputStream ois;
            Collection<NameSpace> nsList = nameSpaces.values();
            for (NameSpace ns : nsList) {
                if (pausedSpaces.containsKey(ns.getId())) {
                    if (!lockService.isLocked(ns.getId())) {
                        runUpdate(ns.getId());
                    } else {
                        continue;
                    }
                }

                internalActualNameSpace = ns.getId().intern();
                error = true;
                retry = 0;
                while (error && retry < 3) {
                    //TODO: use depskyAccessor.readSNSFromDepSky(idPath, accInfo);

                    byte[] array = accessor.readNS(ns.getId().concat("#").concat(ns.getId()), ns.getSnsInfo());
                    if (array != null) {
                        try {
                            bais = new ByteArrayInputStream(array);
                            ois = new ObjectInputStream(bais);

                            NameSpaceRepresentation nsRep = new NameSpaceRepresentation();
                            nsRep.readExternal(ois);

                            IOUtil.closeStream(bais);
                            IOUtil.closeStream(ois);

                            bais = new ByteArrayInputStream(nsRep.getSerializedNS());
                            ois = new ObjectInputStream(bais);
                            auxNs = new NameSpace();
                            auxNs.readExternal(ois);

                            IOUtil.closeStream(bais);
                            IOUtil.closeStream(ois);

                            if (auxNs.getNumberOfNodes() == 0) {
                                Printer.println("UPDATER_TASK: removi o NS: " + auxNs.getId(), "azul");
                                nameSpaces.remove(auxNs.getId());
                                pausedSpaces.remove(auxNs.getId());
                                break;
                            }

//							for(NodeMetadata nm : auxNs.getAllNodes()){
//								System.out.println("-> " + nm.getPath());
//								if(!nm.getPath().equals("/") && !auxNs.putChildren(nm)){
//									NameSpace parentNs = dis.getNS(nm.getParent());
//									DirectoryNodeMetadata parent = (DirectoryNodeMetadata)parentNs.getMetadata(nm.getParent());
//									parent.swapChildren(nm);
//								}
//							}
                            long nsVersion = ns.getVersion();
                            long auxVersion = auxNs.getVersion();

                            boolean containsHash = nShashs.containsKey(auxNs.getId());
                            String localHash = nShashs.get(auxNs.getId());
                            String nsRepHash = nsRep.getHash();

                            if (nsVersion < auxVersion || (nsVersion == auxVersion && (!containsHash || !localHash.equals(nsRepHash)))) {
                                System.out.println("- substituí! - " + auxNs.getId());
                                dis.updateNS(auxNs);
                                nameSpaces.put(ns.getId(), auxNs);
                                nShashs.put(auxNs.getId(), nsRep.getHash());
                            }

                        } catch (IOException e) {
                            System.err.println("StreamCorrupted: " + e.getLocalizedMessage());
                            e.printStackTrace();
                            retry++;
                        } catch (ClassNotFoundException e) {
                        }
                        error = false;
                    } else {
                        Printer.println("NameSpace Updater: SNS is null. " + ns.getId());
                        retry++;
                    }
                    Printer.println("NameSpace Updater: SNS [" + ns.getId() + "] - v. " + ns.getVersion(), "azul");
                }
            }
        }

    }

    public void stopUpdate(String nsId) {
        pausedSpaces.put(nsId, true);
        while (internalActualNameSpace == nsId) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void runUpdate(String nsId) {
        pausedSpaces.remove(nsId);
    }

    public void removeNS(String nsId) {
        nameSpaces.remove(nsId);
    }

}
