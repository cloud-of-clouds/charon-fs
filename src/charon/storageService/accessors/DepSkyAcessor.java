package charon.storageService.accessors;

import java.util.LinkedList;
import java.util.List;

import charon.configuration.CharonConfiguration;
import charon.configuration.storage.cloud.CoCConfiguration;
import charon.directoryService.DirectoryServiceImpl;
import charon.general.CharonConstants;
import charon.general.NSAccessInfo;
import charon.general.Printer;
import charon.general.Statistics;
import charon.storageService.DiskCacheManager;
import charon.util.ExternalMetadataDummy;
import charon.util.compress.DeflaterCompressionUtil;
import depsky.client.CloudsCredentials;
import depsky.client.DataUnit;
import depsky.client.DepSkyClient;
import depsky.client.messages.metadata.ExternalMetadata;
import depsky.util.Pair;
import exceptions.ClientServiceException;
import exceptions.StorageCloudException;

public class DepSkyAcessor implements IMetadataAccessor {

    private DepSkyClient desky;
    private DiskCacheManager disk;
    private int clientId;
    private DirectoryServiceImpl diS;
    private boolean useCompression;

    private static DepSkyAcessor instance;
    private static DepSkyAcessor directInstance;

    //	//Called by garbage collector
    //	private DepSkyAcessor(int clientId, CharonConfiguration config, CoCConfiguration cocConfig){
    //		this(clientId, config, cocConfig, null, CharonConstants.DEFAULT_NUMBER_OF_THREADS);
    //	}
    //
    //	private DepSkyAcessor(int clientId, CharonConfiguration config, CoCConfiguration cocConfig, DirectoryServiceImpl diS){
    //		this(clientId, config, cocConfig, diS,  CharonConstants.DEFAULT_NUMBER_OF_THREADS);
    //	}
    private DepSkyAcessor(int clientId, CharonConfiguration config, CoCConfiguration cocConfig, DirectoryServiceImpl diS, int numberOfThreads) {
        this.clientId = clientId;
        this.diS = diS;
        this.useCompression = config.useCompression();
        try {
            this.desky = new DepSkyClient(clientId, true, true, numberOfThreads, cocConfig.getDepSkyFormatCredentials());
        } catch (StorageCloudException e) {
            e.printStackTrace();
        }
        this.disk = new DiskCacheManager(config.getCacheDirectory());
    }

    //	private DepSkyAcessor(int clientId, CharonConfiguration config, CoCConfiguration cocConfig, DirectoryServiceImpl diS, ICloudServiceLock[] locks){
    //		this.clientId = clientId;
    //		this.diS=diS;
    //		try {
    //			this.desky =  new DepSkyClient(clientId, true, true, CharonConstants.DEFAULT_NUMBER_OF_THREADS, cocConfig.getDepSkyFormatCredentials(), locks);
    //		} catch (StorageCloudException e) {
    //			e.printStackTrace();
    //		}
    //		this.disk = new DiskCacheManager(config.getCacheDirectory());
    //	}
    public static DepSkyAcessor getInstance(int clientId, CharonConfiguration config, CoCConfiguration cocConfig, DirectoryServiceImpl diS) {
        if (instance == null) {
            instance = new DepSkyAcessor(clientId, config, cocConfig, diS, config.getNumOfDepSkyDataThreads());
        }
        return instance;
        //		return new DepSkyAcessor(clientId, config, cocConfig, diS, config.getNumOfDepSkyDataThreads());
    }

    public static DepSkyAcessor getDirectInstance(int clientId, CharonConfiguration config, CoCConfiguration cocConfig, DirectoryServiceImpl diS) {
        if (directInstance == null) {
            directInstance = new DepSkyAcessor(clientId, config, cocConfig, diS, config.getNumOfDepSkyMetadataThreads());
        }
        return directInstance;
        //		return new DepSkyAcessor(clientId, config, cocConfig, diS, config.getNumOfDepSkyMetadataThreads());
    }

    public byte[] readFromDepSky(String fileId) {

        String[] name = fileId.split("#", 2);
        byte[] data = null;
        try {
            //			Printer.println("  -> Start download at depsky", "verde");
            long acMil = System.currentTimeMillis();
            NSAccessInfo snsInfo = diS.getSNSAccessInfo(name[0]);
            if (snsInfo == null || snsInfo.isNSPrivate() || snsInfo.getUsingTheSameAccountsAsOwner()) {
                data = desky.read(new DataUnit(name[0], name[1]));
            } else if (snsInfo.getOwnerId() == this.clientId) {
                data = desky.read(new DataUnit(name[0], name[1], new CloudsCredentials(snsInfo.getCredToAccessSNSOwnedByMe())));
            } else {
                data = desky.read(new DataUnit(name[0], name[1], new CloudsCredentials(snsInfo.getCredToAccessSNSOwnedByOthers())));
            }

            long tempo = System.currentTimeMillis() - acMil;
            Statistics.readCoC(tempo, data == null ? 0 : data.length);

            if (data == null) {
                Printer.println("  -> DepSky: read " + name[1] + " - NULL response. [took: " + Long.toString(tempo) + " ms].", "verde");
            } else {
                Printer.println("  -> DepSky: read " + name[1] + " - OK! [took: " + Long.toString(tempo) + " ms].", "verde");
            }
        } catch (Exception e) {
            Printer.println("  -> DepSky: read " + name[1] + " - Read Error.", "verde");
        }
        return data;
    }

    public byte[] readSNSFromDepSky(String fileId, NSAccessInfo sns) {

        String[] name = fileId.split("#", 2);
        byte[] data = null;
        try {
            //			Printer.println("  -> Start download at depsky", "verde");
            long acMil = System.currentTimeMillis();
            if (sns.getUsingTheSameAccountsAsOwner()) {
                data = desky.read(new DataUnit(name[0], name[1]));
            } else if (sns.getOwnerId() == this.clientId) {
                data = desky.read(new DataUnit(name[0], name[1], new CloudsCredentials(sns.getCredToAccessSNSOwnedByMe())));
            } else {
                data = desky.read(new DataUnit(name[0], name[1], new CloudsCredentials(sns.getCredToAccessSNSOwnedByOthers())));
            }

            long tempo = System.currentTimeMillis() - acMil;
            Statistics.readCoC(tempo, data == null ? 0 : data.length);

            if (data == null) {
                Printer.println("  -> DepSky: read " + name[1] + " - NULL response. [took: " + Long.toString(tempo) + " ms].", "verde");
            } else {
                Printer.println("  -> DepSky: read " + name[1] + " - OK! [took: " + Long.toString(tempo) + " ms].", "verde");
            }

            //Printer.println("  -> DepSky: read " +name[1]+" - [took: " + Long.toString(tempo) + " ms]", "verde");
        } catch (Exception e) {
        	e.printStackTrace();
            Printer.println("  -> DepSky: read " + name[1] + " - Read Error.", "verde");
        }

        return data;
    }

    public byte[] readMatchingFromDepSky(String fileId, ExternalMetadata versionInfo) {
        String[] name = fileId.split("#", 2);
        byte[] data = null;
        try {
            //Printer.println("  -> Start download at depsky", "verde");
            long acMil = System.currentTimeMillis();
            NSAccessInfo snsInfo = diS.getSNSAccessInfo(name[0]);
            if (snsInfo.isNSPrivate() || snsInfo.getUsingTheSameAccountsAsOwner()) {
                data = desky.readDataOnly(new DataUnit(name[0], name[1]), versionInfo);
            } else if (snsInfo.getOwnerId() == this.clientId) {
                data = desky.readDataOnly(new DataUnit(name[0], name[1], new CloudsCredentials(snsInfo.getCredToAccessSNSOwnedByMe())), versionInfo);
            } else {
                data = desky.readDataOnly(new DataUnit(name[0], name[1], new CloudsCredentials(snsInfo.getCredToAccessSNSOwnedByOthers())), versionInfo);
            }

            long tempo = System.currentTimeMillis() - acMil;
            Statistics.readCoC(tempo, data == null ? 0 : data.length);

            if (data != null && useCompression) {
                data = DeflaterCompressionUtil.decompress(data);
            }

            if (data == null) {
                Printer.println("  -> DepSky: read " + name[1] + " - NULL response. [took: " + Long.toString(tempo) + " ms].", "verde");
            } else {
                Printer.println("  -> DepSky: read " + name[1] + " - OK! [took: " + Long.toString(tempo) + " ms].", "verde");
            }

            //Printer.println("  -> End download at depsky", "verde");
            //Printer.println("  -> Download operation took: " + Long.toString(tempo) + " milis", "verde");
        } catch (Exception e) {
            Printer.println("  -> DepSky: read " + name[1] + " - Read Error.", "verde");
        }

        return data;
    }

    /**
     * write data to DepSky
     *
     * @param fileId - in the format [NameSpaceId]#[fileId]
     * @param value - data to write
     * @return a base64 Hex String representation of the hash of the given value.
     */
    public ExternalMetadata writeToDepSky(String fileId, byte[] value, ExternalMetadata versionInfo, boolean isMetadata) {
        String[] name = fileId.split("#", 2);
        try {
            //			Printer.println("  -> Start upload at depsky", "verde");
            long acMil = System.currentTimeMillis();
            ExternalMetadata hash = null;
            NSAccessInfo snsInfo = diS.getSNSAccessInfo(name[0]);
            int cont = 5;
            while (hash == null && cont > 0) {
                try {
                    CloudsCredentials credentials = null;
                    if (snsInfo == null) {
                        return null;
                    }
                    if (snsInfo.isNSPrivate() || snsInfo.getUsingTheSameAccountsAsOwner()) {
                        credentials = null;
                    } else if (snsInfo.getOwnerId() == this.clientId) {
                        credentials = new CloudsCredentials(snsInfo.getCredToAccessSNSOwnedByMe());
                    } else {
                        credentials = new CloudsCredentials(snsInfo.getCredToAccessSNSOwnedByOthers());
                    }
                    if (isMetadata) {
                        String res = desky.write(new DataUnit(name[0], name[1], credentials), value);
                        if (res != null) {
                            hash = new ExternalMetadataDummy(res);
                        }
                    } else {
                        hash = desky.writeDataOnly(new DataUnit(name[0], name[1], credentials), value, (versionInfo == null || versionInfo.getWholeDataHash().equals("")) ? null : versionInfo);
                    }
                } catch (Exception e) {
                    cont--;
                    hash = null;
                }
            }
            long tempo = System.currentTimeMillis() - acMil;
            Statistics.writeCoC(tempo, value.length);

            if (hash == null) {
                Printer.println("  -> DepSky: write " + name[1] + " - NULL response. [took: " + Long.toString(tempo) + " ms].", "verde");
            } else {
                Printer.println("  -> DepSky: write " + name[1] + " - OK! [took: " + Long.toString(tempo) + " ms].", "verde");
            }

            return hash;
        } catch (Exception e) {
            //e.printStackTrace();
            Printer.println("  -> DepSky: write " + name[1] + " - Write Error.", "verde");
        }
        return null;
    }

    public ExternalMetadata writeToDepSky(String fileId, ExternalMetadata versionInfo, boolean isMetadata) {
        byte[] value = disk.readWhole(fileId);
        if (value == null) {
            return null;
        }

        System.out.println("value length pre-compress = " + value.length);
        if (useCompression) {
            value = DeflaterCompressionUtil.compress(value);
        }
        System.out.println("compress lenght = " + value.length);

        return writeToDepSky(fileId, value, versionInfo, isMetadata);

        //		String[] name = fileId.split("#",2);
        //		try {
        //Printer.println("  -> Start upload at depsky file " + name[1], "verde");
        //		long acMil = System.currentTimeMillis();
        //		byte[] value = disk.readWhole(fileId);
        //		if( value == null)
        //			return null;
        //			String hash = null;
        //			NSAccessInfo snsInfo = diS.getSNSAccessInfo(name[0]);
        //			int cont = 5;
        //			while(hash == null && cont > 0){
        //				try{
        //					if(snsInfo.isNSPrivate())
        //						hash = desky.write(new DataUnit(name[0], name[1]), value);
        //					else if(snsInfo.getOwnerId() == this.clientId)		
        //						hash = desky.write(new DataUnit(name[0], name[1], new CloudsCredentials(snsInfo.getCredToAccessSNSOwnedByMe())), value);
        //					else{
        //						hash = desky.write(new DataUnit(name[0], name[1], new CloudsCredentials(snsInfo.getCredToAccessSNSOwnedByOthers())), value);
        //					}
        //				}catch(Exception e){
        //					cont--;
        //					hash=null;
        //				}
        //			}
        //			long tempo = System.currentTimeMillis() - acMil;
        //			Statistics.incWrite(tempo, value.length);
        //
        //
        //			if(hash==null)
        //				Printer.println("  -> DepSky: write " +name[1]+" - NULL response. [took: " + Long.toString(tempo) + " ms].", "verde");
        //			else{
        //				Printer.println("  -> DepSky: write " +name[1]+" - OK! [took: " + Long.toString(tempo) + " ms].", "verde");
        //			}
        //
        //			return hash;
        //		} catch (Exception e) {
        //			Printer.println("  -> DepSky: write " +name[1]+" - Write Error.", "verde");
        //		}
        //		return null;
    }

    public int delete(String fileId) {
        return 0;
    }

    public int garbageCollection(String fileId, int numVersionToKeep) {
        String[] name = fileId.split("#", 2);
        try {
			desky.garbageCollect(new DataUnit(name[0], name[1]), numVersionToKeep);
		} catch (ClientServiceException e) {
			e.printStackTrace();
		}
        return 0;
    }

    public List<Pair<String, String[]>> setPermission(String fileId, String permission, LinkedList<Pair<String, String[]>> cannonicalIds) {
        DataUnit dataU = new DataUnit(fileId, fileId);
        try {
            long acMil = System.currentTimeMillis();

            CloudsCredentials list = desky.setAcl(dataU, permission, new CloudsCredentials(cannonicalIds));
            long tempo = System.currentTimeMillis() - acMil;

            if (list == null) {
                Printer.println("  -> DepSky: set Acl " + fileId + " - NULL response. [took: " + Long.toString(tempo) + " ms].", "verde");
            } else {
                Printer.println("  -> DepSky: set Acl " + fileId + " - OK! [took: " + Long.toString(tempo) + " ms].", "verde");
            }
            return list == null ? null : list.getAsList();
        } catch (Exception e) {
            Printer.println("  -> DepSky: setAcl " + fileId + " - setAcl Error.", "verde");

            return null;

        }
    }

    public void unlock(String nsId) {
        NSAccessInfo snsInfo = diS.getSNSAccessInfo(nsId);
            CloudsCredentials credentials = null;
            if (snsInfo == null) {
                System.out.println("DepSkyAccessor UNLOCK: snsInfo is Null.");
                return;
            }
            if (snsInfo.isNSPrivate() || snsInfo.getUsingTheSameAccountsAsOwner()) {
                credentials = null;
            } else if (snsInfo.getOwnerId() == this.clientId) {
                credentials = new CloudsCredentials(snsInfo.getCredToAccessSNSOwnedByMe());
            } else {
                credentials = new CloudsCredentials(snsInfo.getCredToAccessSNSOwnedByOthers());
            }
        DataUnit dataU = new DataUnit(nsId, nsId, credentials);
        try {
			desky.release(dataU);
		} catch (StorageCloudException e) {
			e.printStackTrace();
		}
    }

    @Override
    public boolean lock(String nsId, int duration) {
        try {
            NSAccessInfo snsInfo = diS.getSNSAccessInfo(nsId);
            CloudsCredentials credentials = null;
            if (snsInfo == null) {
                System.out.println("DepSkyAccessorLock: snsInfo is Null.");
                return false;
            }
            if (snsInfo.isNSPrivate() || snsInfo.getUsingTheSameAccountsAsOwner()) {
                credentials = null;
            } else if (snsInfo.getOwnerId() == this.clientId) {
                credentials = new CloudsCredentials(snsInfo.getCredToAccessSNSOwnedByMe());
            } else {
                credentials = new CloudsCredentials(snsInfo.getCredToAccessSNSOwnedByOthers());
            }

            DataUnit dataU = new DataUnit(nsId, nsId, credentials);
            return desky.lease(dataU, duration, CharonConstants.LOCK_RETRIES);
        } catch (Exception e) {
            System.out.println("(-) DepSkyAccessor: lock(" + nsId + ") - " + e.getMessage());
        }
        return false;
    }

    @Override
    public byte[] readNS(String idPath, NSAccessInfo accInfo) {
        return accInfo == null ? readFromDepSky(idPath) : readSNSFromDepSky(idPath, accInfo);
    }

    @Override
    public boolean writeNS(String idPath, byte[] value) {
        return writeToDepSky(idPath, value, null, true) != null;
    }

}
