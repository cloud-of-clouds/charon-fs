package charon.storageService.accessors;

import java.util.LinkedList;
import java.util.List;

import charon.configuration.CharonConfiguration;
import charon.directoryService.DirectoryServiceImpl;
import charon.general.NSAccessInfo;
import charon.general.Printer;
import charon.general.Statistics;
import charon.storageService.DiskCacheManager;
import charon.util.compress.DeflaterCompressionUtil;
import depsky.clouds.amazon.AmazonS3Driver;
import depsky.util.Pair;
import depsky.util.integrity.IntegrityManager;
import exceptions.StorageCloudException;

public class AmazonAcessor implements IMetadataAccessor {

    private AmazonS3Driver driver;
    private DiskCacheManager disk;
    private DirectoryServiceImpl diS;
    private int clientId;
    private boolean useCompression;

    public AmazonAcessor(int clientId, String accessKey, String secretKey, CharonConfiguration config) {
        this(clientId, accessKey, secretKey, config, null);
    }

    public AmazonAcessor(int clientId, String accessKey, String secretKey, CharonConfiguration config, DirectoryServiceImpl diS) {

        this.disk = new DiskCacheManager(config.getCacheDirectory());
        this.clientId = clientId;
        this.diS = diS;
        this.useCompression = config.useCompression();
        try {
            this.driver = new AmazonS3Driver(clientId, "amazon", accessKey, secretKey);
        } catch (StorageCloudException e) {
            e.printStackTrace();
        }

    }

    public byte[] readMatchingFrom(String fileId, String hash) {

        String[] names = fileId.split("#", 2);
        try {
            String name = names[1].concat(hash);
            //Printer.println("  -> Start download from AmazonS3", "verde");
            long acMil = System.currentTimeMillis();

            String[] accessInfo = null;
            NSAccessInfo snsInfo = diS.getSNSAccessInfo(names[0]);
            if (snsInfo.isNSPrivate() || snsInfo.getUsingTheSameAccountsAsOwner()) {
                accessInfo = null;
            } else if (snsInfo.getOwnerId() == this.clientId) {
                accessInfo = snsInfo.getCredToAccessSNSOnAmazon();
            } else {
                accessInfo = snsInfo.getCredToAccessSNSOnAmazon();
            }

            byte[] value = driver.readObjectData(names[0], name, accessInfo);
            long tempo = System.currentTimeMillis() - acMil;
            Statistics.readSingleCloud(tempo, value == null ? 0 : value.length);

            if (value != null && useCompression) {
                value = DeflaterCompressionUtil.decompress(value);
            }

            if (value == null) {
                Printer.println("  -> Amazon S3: read " + names[1] + " - NULL response. [took: " + Long.toString(tempo) + " ms].", "verde");
            } else {
                Printer.println("  -> Amazon S3: read " + names[1] + " - OK! [took: " + Long.toString(tempo) + " ms].", "verde");
            }

            //Printer.println("  -> End download from AmazonS3", "verde");
            //Printer.println("  -> Download operation took: " + Long.toString(tempo) + " milis", "verde");
            if (hash.equals(IntegrityManager.getHexHash(value)));
            return value;
        } catch (Exception e) {
            Printer.println("  -> Amazon S3: read " + names[1] + " - Read Error.", "verde");
        }

        return null;
    }

    public String writeTo(String fileId) {
        String[] names = fileId.split("#", 2);
        try {
            byte[] value = disk.readWhole(fileId);
            if (value == null) {
                return null;
            }

            if (useCompression) {
                value = DeflaterCompressionUtil.compress(value);
            }

            String hash = IntegrityManager.getHexHash(value);
            String name = names[1].concat(hash);
            long acMil = System.currentTimeMillis();
//            System.out.println("  -> Start upload at amazon file " + names[1]);
            NSAccessInfo snsInfo = diS.getSNSAccessInfo(names[0]);
            if (snsInfo.isNSPrivate() || snsInfo.getUsingTheSameAccountsAsOwner()) {
                driver.writeObject(names[0], name, value, null);
            } else if (snsInfo.getOwnerId() == this.clientId) {
                driver.writeObject(names[0], name, value, snsInfo.getCredToAccessSNSOnAmazon());
            } else {
                driver.writeObject(names[0], name, value, snsInfo.getCredToAccessSNSOnAmazon());
            }

            long tempo = System.currentTimeMillis() - acMil;
            Statistics.writeCoC(tempo, value.length);

            Printer.println("  -> Amazon S3: write " + names[1] + " - OK! [took: " + Long.toString(tempo) + " ms].", "verde");
            return hash;
        } catch (StorageCloudException e) {
            //e.printStackTrace();
            Printer.println("  -> Amazon S3: write " + names[1] + " - Write Error.", "verde");
        }
        return null;
    }

    public List<Pair<String, String[]>> setPermission(String folderId, String permition,
            LinkedList<Pair<String, String[]>> cannonicalIds) {
        String[] cannonicalAmazon = null;
        for (Pair<String, String[]> cannonicals : cannonicalIds) {
            if (cannonicals.getKey().equals("AMAZON-S3")) {
                cannonicalAmazon = cannonicals.getValue();
            }
        }
        try {
            driver.setAcl(folderId, cannonicalAmazon, permition);
        } catch (StorageCloudException e) {
            return null;
        }
        return cannonicalIds;
    }

    public int delete(String fileId) {
        String[] names = fileId.split("#", 2);
        try {
            List<String> list = driver.listContainer(names[1], names[0], null);
            String[] ids = new String[list.size()];
            int i = 0;
            for (String id : list) {
                ids[i] = id;
                i++;
            }
            driver.deleteObjects(names[0], ids, null);
        } catch (StorageCloudException e) {
            System.out.println("ERROR deleting from AWS S3!");
        }
        return 0;
    }

    public int delete(String fileId, String hash) {
        try {
            String[] names = fileId.split("#", 2);
            String name = names[1].concat(hash);
            driver.deleteObject(null, name, null);
        } catch (StorageCloudException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public int garbageCollection(String fileId, int numVersionToKeep) {
        //TODO: IMPLEMENT
        return 0;
    }

    @Override
    public byte[] readNS(String nsId, NSAccessInfo accInfo) {
        String idPath = nsId.split("#", 2)[0];
        try {
            String[] uploadToOtherAccounts = null;
            if (accInfo == null || accInfo.isNSPrivate() || accInfo.getUsingTheSameAccountsAsOwner()) {
                uploadToOtherAccounts = null;
            } else if (accInfo.getOwnerId() == this.clientId) {
                uploadToOtherAccounts = accInfo.getCredToAccessSNSOnAmazon();
            } else {
                uploadToOtherAccounts = accInfo.getCredToAccessSNSOnAmazon();
            }
            long tempo = System.currentTimeMillis();
            byte[] value = driver.readObjectData(idPath, idPath, uploadToOtherAccounts);
            tempo = System.currentTimeMillis() - tempo;
            Statistics.readSingleCloud(tempo, value == null ? 0 : value.length);

            if (value == null) {
                Printer.println("  -> Amazon S3: read " + idPath + " - NULL response. [took: " + Long.toString(tempo) + " ms].", "verde");
            } else {
                Printer.println("  -> Amazon S3: read " + idPath + " - OK! [took: " + Long.toString(tempo) + " ms].", "verde");
            }

            return value;

        } catch (Exception ex) {
            Printer.println("  -> Amazon S3: read " + idPath + " - Read Error.", "verde");
        }
        return null;
    }

    @Override
    public boolean writeNS(String nsId, byte[] value) {
        String idPath = nsId.split("#", 2)[0];
        try {
            NSAccessInfo accInfo = diS.getSNSAccessInfo(idPath);
            String[] uploadToOtherAccounts = null;
            if (accInfo == null || accInfo.isNSPrivate() || accInfo.getUsingTheSameAccountsAsOwner()) {
                uploadToOtherAccounts = null;
            } else if (accInfo.getOwnerId() == this.clientId) {
                uploadToOtherAccounts = accInfo.getCredToAccessSNSOnAmazon();
            } else {
                uploadToOtherAccounts = accInfo.getCredToAccessSNSOnAmazon();
            }

//            Printer.println("  -> Start upload at amazon file " + idPath, "verde");
            long tempo = System.currentTimeMillis();
            driver.writeObject(idPath, idPath, value, uploadToOtherAccounts);
            tempo = System.currentTimeMillis() - tempo;
            Statistics.writeSingleCloud(tempo, value == null ? 0 : value.length);
            Printer.println("  -> Amazon S3: write " + idPath + " - OK! [took: " + Long.toString(tempo) + " ms].", "verde");

            return true;
        } catch (Exception ex) {
            ex.printStackTrace();
            Printer.println("  -> Amazon S3: write " + idPath + " - Write Error.", "verde");
        }
        return false;
    }

    @Override
    public boolean lock(String ns, int time) {

        String[] accessInfo = null;
        NSAccessInfo snsInfo = diS.getSNSAccessInfo(ns);
        if (snsInfo.isNSPrivate() || snsInfo.getUsingTheSameAccountsAsOwner()) {
            accessInfo = null;
        } else if (snsInfo.getOwnerId() == this.clientId) {
            accessInfo = snsInfo.getCredToAccessSNSOnAmazon();
        } else {
            accessInfo = snsInfo.getCredToAccessSNSOnAmazon();
        }

        return driver.lock(ns, ns, time, accessInfo);
    }

}
