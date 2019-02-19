package charon.storageService;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import charon.configuration.CharonConfiguration;
import charon.configuration.Location;
import charon.configuration.storage.cloud.SingleCloudConfiguration;
import charon.directoryService.DirectoryServiceImpl;
import charon.directoryService.NodeMetadata;
import charon.directoryService.exceptions.DirectoryServiceException;
import charon.general.Charon;
import charon.lockService.LockService;
import charon.storageService.accessors.AmazonAcessor;
import charon.storageService.accessors.DepSkyAcessor;
import charon.storageService.accessors.ExternalRepositoryAccessor;
import charon.storageService.accessors.PrivateRepositoryAcessor;
import charon.storageService.externalSendThread.ExternalBlocksSendThread;
import charon.storageService.externalSendThread.ExternalObjectToSend;
import charon.storageService.scpSockets.SCPSocketServer;
import charon.util.ExternalMetadataDummy;
import depsky.client.messages.metadata.ExternalMetadata;
import depsky.util.Pair;

public class StorageService implements IStorageService {

    //	16MB
    public static final int blockSize = 16777216;
    private boolean usePrefetching;
    private boolean useMemoryCache;
    private DataStatsManager dataManager;
    private DiskCacheManager diskManager;
    private MemoryCacheManager memoryManager;
    private SendingQueue queue;
    private DepSkyAcessor depskyDirect;
    private AmazonAcessor amazon;
    private PrivateRepositoryAcessor localRep;
    private DirectoryServiceImpl diS;
    private LockService loS;
    private LinkedList<String> releasedFiles;
    private LinkedList<PreFetchingThread> preFetchingThreads;
    private ConcurrentHashMap<String, Integer> preFetchingCurrentRun;
    private ConcurrentHashMap<String, Integer> preFetchingAvaliation;
    private ConcurrentHashMap<String, LinkedList<Integer>> numberOfEnqueuedBlocksPerFlush;
    private LinkedBlockingQueue<Pair<String, Pair<Location, ExternalMetadata>>> jobs;
    private ConcurrentLinkedQueue<String> prefetchingErrorAnalizer;

    private boolean isTimes;

    private ExternalRepositoryAccessor externalRep;
    private BlockingQueue<ExternalObjectToSend> externalQueue;

    public StorageService(int clientId, CharonConfiguration config, int numOfThreads,
            int memorySize, DirectoryServiceImpl diS, LockService loS) {

        File cache = new File(config.getCacheDirectory());
        while (!cache.exists()) {
            cache.mkdirs();
        }
        this.dataManager = new DataStatsManager(config.getCacheDirectory());
        this.diskManager = new DiskCacheManager(config.getCacheDirectory());

        if (config.getCoCConfiguration() != null) {
            this.depskyDirect = DepSkyAcessor.getDirectInstance(clientId, config, config.getCoCConfiguration(), diS);
        }

        SingleCloudConfiguration cred = config.getSingleCloudConfig();

        if (cred != null) {
            this.amazon = new AmazonAcessor(clientId, cred.getAccessKey(), cred.getSecretKey(), config, diS);
        }

        if (config.getPrivateRepConfig() != null) {
            this.localRep = new PrivateRepositoryAcessor(config, diS);
        }

        //TO DEAL WITH EXTERNAL FILES.
        if (config.getExternalRepConfig() != null) {
            this.externalRep = new ExternalRepositoryAccessor(config, diS);
            this.externalQueue = new LinkedBlockingQueue<ExternalObjectToSend>();

            new ExternalBlocksSendThread(externalQueue, externalRep, this).start();
        }
        // ======================

        this.diS = diS;
        this.loS = loS;
        this.releasedFiles = new LinkedList<String>();
        this.preFetchingThreads = new LinkedList<PreFetchingThread>();
        this.jobs = new LinkedBlockingQueue<Pair<String, Pair<Location, ExternalMetadata>>>();
        if (config.usePrefecting()) {
            for (int i = 0; i < config.getNumOfPrefetchingThreads(); i++) {
                PreFetchingThread pre = new PreFetchingThread(clientId, config, this, diS, jobs);
                pre.start();
                preFetchingThreads.add(pre);
            }
        }
        this.preFetchingAvaliation = new ConcurrentHashMap<String, Integer>();
        this.preFetchingCurrentRun = new ConcurrentHashMap<String, Integer>();
        this.prefetchingErrorAnalizer = new ConcurrentLinkedQueue<String>();
        this.numberOfEnqueuedBlocksPerFlush = new ConcurrentHashMap<String, LinkedList<Integer>>();
        this.useMemoryCache = config.isUseMainMemoryCache();
        this.usePrefetching = config.usePrefecting();
        if (useMemoryCache) {
            this.memoryManager = new MemoryCacheManager(memorySize, diskManager);
        }
        this.queue = new SendingQueue(this, numOfThreads, clientId, config);
        this.isTimes = config.isTimes();

//        if(localRep != null || externalRep!= null)
        new SCPSocketServer(config.getRemoteRepositoryServerPort(), this, localRep == null ? null : localRep.getFileRepository(), externalRep == null ? null : externalRep.getFileRepository()).start();
    }

    @Override
    public long writeData(String fileId, ByteBuffer buf, int offset, boolean isPending,
            long totalSize, Map<Integer, ExternalMetadata> hashs, Location location) {
        DataStats dt = dataManager.getDataStats(fileId);
        if (dt == null) {
            dt = dataManager.newDataStats(fileId);
        }

        int totalValueSize = buf.capacity();

        int block = getBlockNumber(offset);
        int block_offset = offset - ((block - 1) * blockSize);
        int numOfBlocksToUse = (getBlockNumber(offset + totalValueSize) - block) + 1;
        int valueSize = totalValueSize;
        int len;
        for (int i = 0; i < numOfBlocksToUse && valueSize > 0; i++) {
            if (i == numOfBlocksToUse - 1) {
                len = valueSize;
            } else {
                len = blockSize - block_offset;
            }

            if (offset < totalSize && updateCache(fileId, !hashs.containsKey(block - 1) ? new ExternalMetadataDummy("") : hashs.get(block - 1), isPending, block, totalSize, location, null) != 0) {
                System.out.println("UPDATECACHE --- returning -1");
                return -1;
            }

            writeInCache(fileId, block, block_offset, buf, len, dt);
            valueSize -= len;
            block_offset = 0;
            block++;
        }

        if (offset + totalValueSize > totalSize) {
            dataManager.setToTalSize(fileId, offset + totalValueSize);
        } else {
            dataManager.setToTalSize(fileId, totalSize);
        }

        if (dt.getNumberOfBlocks() == block) {
            dataManager.setSize(fileId, block_offset + totalValueSize, false);
        }
        return dataManager.getDataStats(fileId).getTotalSize();
    }

    private void writeInCache(String fileId, int block, int block_offset, ByteBuffer buf, int len, DataStats dt) {
        String block_fileId = fileId.concat("_".concat(block + ""));
        long time;
        if (!dt.getWriteInClouds(block)) {
            dataManager.setWriteInClouds(fileId, block, true);
        }
        if (useMemoryCache) {
            time = System.nanoTime();
            memoryManager.write(block_fileId, buf, block_offset, len);
            if (!dt.getWriteInDisk(block)) {
                dataManager.setWriteInDisk(fileId, block, true);
            }
        } else {
            time = System.nanoTime();
            diskManager.write(block_fileId, buf, block_offset, len);
        }

    }

    public void writeInCache(String pathId, byte[] data, String dataHashHex) {
        DataStats dt = dataManager.getDataStats(pathId);
        if (dt == null) {
            dt = dataManager.newDataStats(pathId);
        }
        dt.setHash(new ExternalMetadataDummy(dataHashHex));
        diskManager.writeWhole(pathId, data);
    }

    @Override
    public int readData(String fileId, ByteBuffer buf, int offset,
            int cap, Map<Integer, ExternalMetadata> hashs, boolean isPending,
            long totalSize, Location location, String externalManaged) {

        int capacity = cap;

        if (capacity > totalSize - offset) {
            capacity = (int) totalSize - offset;
        }

        int block = getBlockNumber(offset);
        String block_fileId = fileId.concat("_".concat(block + ""));
        int block_offset = offset - ((block - 1) * blockSize);
        int numOfBlocksToUse = (getBlockNumber(offset + capacity) - block) + 1;
        byte[] data = new byte[capacity];
        int capSize = capacity;
        for (int i = 0; i < numOfBlocksToUse && capSize > 0; i++) {
            if (i == numOfBlocksToUse - 1) {
                capacity = capSize;
            } else {
                capacity = blockSize - block_offset;
            }

            //			if(usePrefetching && location != Location.LOCAL)
            if (usePrefetching) {
                preFetchingAnalizer(block_offset, capacity, block_fileId, hashs, location);
            }
            if (offset < totalSize && updateCache(fileId, !hashs.containsKey(block - 1) ? new ExternalMetadataDummy("") : hashs.get(block - 1), isPending, block, totalSize, location, externalManaged) != 0) {
                return -1;
            }

            int result = readFromCache(block_fileId, capacity, block_offset, data, data.length - capSize, buf);
            if (result == -1) {
                System.out.println(block_fileId + " : block value e null");
                return -1;
            }
            capSize -= capacity;
            block_offset = 0;
            block++;
            block_fileId = fileId.concat("_".concat(block + ""));
        }
        return 0;
    }

    public byte[] readCachedOrLocalRepData(String block_fileId, String dataHashHex) {
        DataStats dt = dataManager.getDataStats(getFileIdFromBlockId(block_fileId));
        if (dt == null) {
            //dt = dataManager.newDataStats(pathId);
            return null;
        }

        if (diskManager.isInCache(block_fileId) && dt.getHash(getBlockFromBlockId(block_fileId)).getWholeDataHash().equals(dataHashHex)) {
            return diskManager.readWhole(block_fileId);
        } else {
            return localRep.read(block_fileId, dataHashHex);
        }
        //!diskManager.isInCache(pathId) ||
    }

    public byte[] readCachedOrExternalRepData(String filename, long offset, String dataHashHex, String externalManaged) {
        String pathId = null;
        try {
            pathId = diS.getMetadata(filename).getIdpath();
        } catch (DirectoryServiceException e) {
            System.out.println("StorageService.readCachedOrExternalRepData(): - filename = " + filename);
            e.printStackTrace();
            return null;
        }

        DataStats dt = dataManager.getDataStats(pathId);
        if (dt == null) {
            //dt = dataManager.newDataStats(pathId);
            return null;
        }

        String block_fileId = pathId + "_" + getBlockNumber(offset);

        if (diskManager.isInCache(block_fileId) && dt.getHash(getBlockFromBlockId(block_fileId)).getWholeDataHash().equals(dataHashHex)) {
            return diskManager.readWhole(block_fileId);
        } else if (externalManaged == null) {
            return externalRep.read(filename, getBlockNumber(offset) * blockSize, dataHashHex);
        } else {
            return externalRep.read(filename, getBlockNumber(offset) * blockSize, dataHashHex, externalManaged);
        }
        //!diskManager.isInCache(pathId) ||
    }

    private boolean preFetchingAnalizer(int offset, int capacity, String block_fileId, Map<Integer, ExternalMetadata> hashs, Location location) {

        if (!preFetchingAvaliation.containsKey(block_fileId) && offset != 0) {
            //vai come�ar a ler o bloco do meio
        } else if (!preFetchingAvaliation.containsKey(block_fileId) && offset == 0) { //vai ler o primeiro byte de um bloco
            int lastByteReadByCurrentReadOp = capacity + offset - 1;
            preFetchingAvaliation.put(block_fileId, lastByteReadByCurrentReadOp);
        } else {//avaliaca se continua a ser acesso sequencial
            int previousLastByteRead = preFetchingAvaliation.get(block_fileId);
            int lastByteReadByCurrentReadOp = capacity + offset - 1;
            int firstByteReadByCurrentReadOp = offset;
            if (firstByteReadByCurrentReadOp == previousLastByteRead + 1) { //continua a ser sequencial
                if (firstByteReadByCurrentReadOp > blockSize / 2) {
                    String fileId = getFileIdFromBlockId(block_fileId);
                    DataStats dt = dataManager.getDataStats(fileId);
                    int block = getBlockFromBlockId(block_fileId);
                    String nextBlock_fileId = null;
                    for (int i = block + 1; i < dt.getNumberOfBlocks(); i++) {
                        nextBlock_fileId = fileId.concat("_".concat(i + ""));
                        if (hashs.get(block - 1) == null || diskManager.isInCache(nextBlock_fileId) && dt.getHash(block).getWholeDataHash().equals(hashs.get(block - 1).getWholeDataHash())) {
                            continue;
                        }
                        if (!preFetchingCurrentRun.containsKey(nextBlock_fileId)) {
                            Pair<String, Pair<Location, ExternalMetadata>> pair = new Pair<String, Pair<Location, ExternalMetadata>>(nextBlock_fileId, new Pair<Location, ExternalMetadata>(location, hashs.get(i - 1)));
                            jobs.offer(pair);
                            preFetchingCurrentRun.put(nextBlock_fileId, i);
                        }
                    }
                    preFetchingAvaliation.remove(block_fileId);
                } else {
                    preFetchingAvaliation.put(block_fileId, lastByteReadByCurrentReadOp);
                }
            }
        }
        return false;
    }

    private int readFromCache(String blockId, int capacity, int block_offset, byte[] buf, int index, ByteBuffer buffer) {
        int res = -1;
        if (useMemoryCache) {
            res = memoryManager.read(blockId, buffer, block_offset, capacity);
        }

        if (res == -1) {
            res = diskManager.read(blockId, buffer, block_offset, capacity);
        }

        return res;
    }

    @Override
    public int truncData(String path, String fileId, int size, Map<Integer, ExternalMetadata> versionsInfo, boolean isToSyncWClouds,
            boolean isPending, long totalSize, Location location) {

        int block = getBlockNumber(size);
        String block_fileId = fileId.concat("_".concat(block + ""));
        int block_size = size - ((block - 1) * blockSize);

        DataStats dt = dataManager.getDataStats(fileId);
        if (dt == null) {
            dt = dataManager.newDataStats(fileId);
        }
        //if(size<totalSize && versionsInfo.containsKey(block-1)) -> nao faz sentido porque na chamada a este metodo (o totalSize é posto igual ao newsize), logo nunca vai entrar no if.
        if (versionsInfo.containsKey(block - 1)) {
            updateCache(fileId, versionsInfo.get(block - 1), isPending, block, totalSize, location, null);
        } else {
            return -1;
        }
        diskManager.truncate(block_fileId, block_size);
        if (useMemoryCache) {
            memoryManager.truncate(block_fileId, block_size);
            dataManager.setWriteInDisk(fileId, block, true);
        }
        dataManager.setWriteInClouds(fileId, block, true);
        dataManager.setSize(fileId, block_size, true);
        dataManager.setToTalSize(fileId, size);
        dataManager.truncateInfo(fileId, block);
        if (location == Location.EXTERNAL_REP) {
            externalRep.truncate(path, size);
        }

        if (isToSyncWClouds) {
            syncWClouds(fileId, location);
        }
        return 0;
    }

    public int deleteData(String fileId, Location location) {
        DataStats dt = dataManager.getDataStats(fileId);
        if (dt != null) {
            int numberOfBlocks = dt.getNumberOfBlocks();
            String block_fileId;
            for (int i = 1; i <= numberOfBlocks; i++) {
                block_fileId = fileId.concat("_".concat(i + ""));
                if (useMemoryCache) {
                    memoryManager.delete(block_fileId);
                }
                if (diskManager.isInCache(block_fileId)) {
                    diskManager.delete(block_fileId);
                }
                if (preFetchingCurrentRun.containsKey(block_fileId)) {
                    preFetchingCurrentRun.remove(block_fileId);
                }
                if (jobs.contains(block_fileId)) {
                    jobs.remove(block_fileId);
                }
//                switch (location) {
//                    case CoC:
//                        depskyDirect.delete(block_fileId);
//                        break;
//                    case SINGLE_CLOUD:
//                        amazon.delete(block_fileId);
//                        break;
//                    default:
//                        break;
//                }
            }
            queue.removeSendingObject(fileId, dt.getNumberOfBlocks());
            numberOfEnqueuedBlocksPerFlush.remove(fileId);
            dataManager.deleteDataStats(fileId);
            System.out.println("FIZ RELEASE : " + fileId);
            loS.release(fileId);
        }

        return 0;
    }

    @Override
    public void syncWDisk(String fileId, Location location) {
        DataStats dt = dataManager.getDataStats(fileId);
        if (dt != null) {
            if (useMemoryCache) {
                String block_fileId;
                for (Entry<Integer, Boolean> e : dt.getAllWriteInDiskValues().entrySet()) {
                    block_fileId = fileId.concat("_".concat(e.getKey() + ""));
                    if (e.getValue()) {
                        byte[] data = memoryManager.readWhole(block_fileId); //to get the whole block
                        if (data == null) {
                            continue;
                        }
                        diskManager.writeWhole(block_fileId, data);
                        dataManager.setWriteInDisk(fileId, e.getKey(), false);
                        if (!dt.getWriteInClouds(e.getKey())) {
                            dataManager.setWriteInClouds(fileId, e.getKey(), true);
                        }
                    }
                }
            }
        }
    }

    @Override
    public int syncWClouds(String fileId, Location location) {

        String flushId = System.nanoTime() + "";

        DataStats dt = dataManager.getDataStats(fileId);
        if (dt != null) {
            syncWDisk(fileId, location);
            String block_fileId;
            ConcurrentHashMap<Integer, Boolean> ifValuesToWrite = dt.getAllWriteInCloudsValues();
            int enqueuedBlocks = 0;

            synchronized (flushId.intern()) {
                for (int i = 1; i <= dt.getNumberOfBlocks(); i++) {
                    block_fileId = fileId.concat("_".concat(i + ""));

                    if (ifValuesToWrite.containsKey(i) && ifValuesToWrite.get(i)) {
                        if (location == Location.EXTERNAL_REP) {
                            NodeMetadata m = diS.getMetadataByPathId(fileId);
                            boolean sent = false;
                            while (!sent) {
                                try {
                                    externalQueue.put(new ExternalObjectToSend(m.getPath(), new Pair<String, Long>(block_fileId, (long) ((i - 1) * blockSize)), flushId));
                                    sent = true;
                                } catch (InterruptedException e) {
                                }
                            }
                            enqueuedBlocks++;
                        } else if (queue.addSendingObject(block_fileId, location, null, dt.getHash(i), flushId)) {
                            enqueuedBlocks++;
                        }
                        dataManager.setWasFlushed(fileId, i, true, true);
                        dataManager.setWriteInClouds(fileId, i, false);
                    }
                }
            }

            if (enqueuedBlocks > 0) {
                if (!numberOfEnqueuedBlocksPerFlush.contains(fileId)) {
                    LinkedList<Integer> toMap = new LinkedList<Integer>();
                    numberOfEnqueuedBlocksPerFlush.put(fileId, toMap);
                }
                numberOfEnqueuedBlocksPerFlush.get(fileId).addLast(enqueuedBlocks);
            }
            return 0;
        }
        return -1;
    }

//	public void directWrite(String fileId, byte[] data, boolean isSync){
//		if(isSync)
//			depskyDirect.writeToDepSky(fileId, data, null, true);
//		else{
//			queue.addSendingObject(fileId, Location.CoC, data, null, fileId);
//		}
//	}
    @Override
    public int updateCache(String fileId, ExternalMetadata versionInfo, boolean isPending, int block, long totalSize, Location location, String externalManaged) {
        String block_fileId = fileId.concat("_".concat(block + ""));
        DataStats dt = dataManager.getDataStats(fileId);
        if (dt == null) {
            dt = dataManager.newDataStats(fileId);
        }
        if (!isPending) {
            byte[] data = null;
            int cont = 0;
            //waits until the block is downloaded
            boolean isPreFetched = false;

            if (usePrefetching) {
                while (preFetchingCurrentRun.containsKey(block_fileId)) {
                    if (cont++ == 10) {
                        System.out.println("--- WAITING FOR DOWNLOAD IN PRE-FETCHING " + block_fileId);
                        System.out.println("jobs size: " + jobs.size());
                        cont = 0;
                    }
                    try {
                        Thread.sleep(100);
                        isPreFetched = true;
                    } catch (InterruptedException e) {

                    }
                }
                if (prefetchingErrorAnalizer.contains(block_fileId)) {
                    prefetchingErrorAnalizer.remove(block_fileId);
                    return -1;
                }
                //updated in the method updataCacheFromPreFetching
                if (isPreFetched) {
                    return 0;
                }
            }

            try {
                if (versionInfo != null && versionInfo.getWholeDataHash().length() != 0 && (!diskManager.isInCache(block_fileId) || !dt.getHash(block).getWholeDataHash().equals(versionInfo.getWholeDataHash()))) {

                    //					System.out.println("===== UPDATE CACHE - " +block_fileId + "=====");
                    //					System.out.println("versionInfo == nulll - " + (versionInfo==null));
                    //					if(versionInfo!=null){
                    //						System.out.println("versionInfo.getWholeDataHash().length() - " + versionInfo.getWholeDataHash().length());
                    //						System.out.println("versionInfo.getWholeDataHash() - " + versionInfo.getWholeDataHash());
                    //					}
                    //					System.out.println("diskManager.isInCache(block_fileId) - " + diskManager.isInCache(block_fileId));
                    //					System.out.println("dt.getHash(block).getWholeDataHash() - " + dt.getHash(block).getWholeDataHash());
                    //					System.out.println("==================================");
                    switch (location) {
                        case CoC:
                            //						System.out.println("vou ler : " + block_fileId + " - " + ((versionInfo instanceof ExternalMetadataDummy) ? "its a DUMMY: " + versionInfo.getWholeDataHash() :  versionInfo));
                            data = depskyDirect.readMatchingFromDepSky(block_fileId, versionInfo);
                            break;
                        case SINGLE_CLOUD:
                            if (amazon != null) {
                                data = amazon.readMatchingFrom(block_fileId, versionInfo.getWholeDataHash());
                            } else {
                                System.out.println("Trying to read data from Single Cloud but no Single Cloud configuration was found.");
                            }
                            break;
                        case PRIVATE_REP:
                            String[] split = block_fileId.split("#");
                            if (split.length >= 2) {
                                //							data = localRep.readFromLocalRep(block_fileId, versionInfo.getWholeDataHash(), new int[] {4,5});
                                data = localRep.read(block_fileId, versionInfo.getWholeDataHash());
                            }
                            break;
                        case EXTERNAL_REP:
                            String filename = diS.getMetadataByPathId(fileId).getPath();
                            if (externalManaged != null) {
                                data = externalRep.read(filename, (block - 1) * blockSize, versionInfo.getWholeDataHash(), externalManaged);
                            } else {
                                data = externalRep.read(filename, (block - 1) * blockSize, versionInfo.getWholeDataHash());
                            }
                            break;
                        default:
                            break;
                    }
                    if (data != null) {
                        diskManager.writeWhole(block_fileId, data);
                        dataManager.setWriteInClouds(fileId, block, false);
                        int numOfBlocks = getBlockNumber(totalSize);
                        if (numOfBlocks == block) {
                            dataManager.setSize(fileId, data.length, false);
                        }
                        dataManager.setToTalSize(fileId, totalSize);
                        dataManager.setHash(fileId, block, versionInfo);
                    } else {
                        return -1;
                    }
                }
            } catch (NullPointerException e) {
                e.printStackTrace();
                System.out.println("==========");
                System.out.println("versionInfo == null - " + (versionInfo == null));
                if (versionInfo != null) {
                    System.out.println("versionInfo.hash == null - " + (versionInfo.getWholeDataHash() == null));
                }
                System.out.println("dt.getHash(block) - " + (dt.getHash(block) == null));
                System.exit(0);
            }

            if (useMemoryCache) {
                if (data != null) {
                    memoryManager.writeWhole(block_fileId, data);
                } else if (!memoryManager.isInCache(block_fileId)) {
                    data = diskManager.readWhole(block_fileId);
                    if (data != null) {
                        memoryManager.writeWhole(block_fileId, data);
                    }
                }
            }

        }
        return 0;
    }

    public void updataCacheFromPreFetching(String block_fileId, ExternalMetadata versionInfo, byte[] data) {
        String fileId = getFileIdFromBlockId(block_fileId);
        System.out.println(Thread.currentThread().getId() + " --  END PRE-FETCHING FOR BLOCK: " + block_fileId);
        if (data != null) {
            int block = getBlockFromBlockId(block_fileId);
            diskManager.writeWhole(block_fileId, data);
            dataManager.setWriteInClouds(fileId, block, false);
            dataManager.setHash(fileId, block, versionInfo);
        }
        //		if(useMemoryCache){
        //			if(data != null){
        //				time = System.nanoTime();
        //				memoryManager.write(block_fileId, data, 0);
        //				Statistics.incWriteInMem(System.nanoTime()-time, data.length);
        //			}else if(!memoryManager.isInCache(block_fileId)){
        //				time = System.nanoTime();
        //				data = diskManager.read(block_fileId, 0, -1);
        //
        //				if(data != null){
        //					Statistics.incReadInDisk(System.nanoTime()-time, data.length);
        //					time = System.nanoTime();
        //					memoryManager.write(block_fileId, data, 0);
        //					Statistics.incWriteInMem(System.nanoTime()-time, data.length);
        //				}
        //			}
        //		}
        preFetchingCurrentRun.remove(block_fileId);
    }

    public void readErrorOnPrefetching(String fileId) {
        prefetchingErrorAnalizer.add(fileId);
        preFetchingCurrentRun.remove(fileId);
    }

    public List<Pair<String, String[]>> setPermission(String fileId, String permission,
            LinkedList<Pair<String, String[]>> cannonicalIds, Location location) {

        List<Pair<String, String[]>> res = null;
        //TODO: turn the single_cloud location dynamic (not only for amazon)
        switch (location) {
            case SINGLE_CLOUD:
                //da permissao para o bucket onde vao ser armazenado os dados no caso de usar so Amazon
                res = amazon.setPermission(fileId, permission, cannonicalIds);
                if (res == null) {
                    return null;
                }
                break;
            default:
                break;
        }
        //		for(Pair<String, String[]> asd : cannonicalIds){
        //			for(String str : asd.getValue()){
        //				System.out.println(asd.getKey() + " - " + str);
        //			}
        //		}
        //da permissao para bucket do SNS na cloud-of-clouds
        res = depskyDirect.setPermission(fileId, permission, cannonicalIds);

        return res;
    }

    @Override
    public int releaseData(String fileId, boolean isPending) {
        //release it if it release it before
        if (releasedFiles.contains(fileId) || isPending) {
            loS.release(fileId);
            //TODO: eliminar blocos de pre-fetching se estão na queue
        } else {
            releasedFiles.add(fileId); // first time release for this fileId
        }
        return 0;
    }

    public synchronized void commit(String blockId, ExternalMetadata hash, Location location, String flushId) {

        String[] names = blockId.split("#");
        if (names[0].equals(names[1].replace("versions", ""))) {
            return;
        }

        try {

            String fileId = getFileIdFromBlockId(blockId);
            int block = getBlockFromBlockId(blockId);
            DataStats dt = dataManager.getDataStats(fileId);
            if (dt == null) { //caso o ficheiro tenha sido eliminado durante o envio. ver depois como fazer para apagar com o garbage colector
                return;
            }
            synchronized (flushId.intern()) {

                dataManager.setWasFlushed(fileId, block, false, true);
                dataManager.setHash(fileId, block, hash);
                int filesPerSending = 0;
                for (Entry<Integer, Boolean> e : dt.getAllBlockFlushed().entrySet()) {
                    if (e.getValue()) {
                        filesPerSending++;
                    }
                }
                int n = 0;
                LinkedList<Integer> obj = numberOfEnqueuedBlocksPerFlush.get(fileId);
                //				System.out.println(" ---- stsCommit :" + blockId + ", hash = " + hash.getWholeDataHash());
                if (obj == null) {
                    System.out.println("NULL NO COMMIT: fileId = " + fileId);
                } else {
                    n = obj.poll();
                    if (n > 0) {
                        n--;
                        numberOfEnqueuedBlocksPerFlush.get(fileId).addFirst(n);
                    }
                }

                //			diS.commitMetadataBuffer(blockId, hash, (filesPerSending == 0 && releasedFiles.contains(fileId)));
                if (n == 0) {
                    System.out.println("-> vai enviar metadados para a cloud");
                }

                diS.commitMetadataBuffer(blockId, hash, (n == 0));
                if (filesPerSending == 0 && releasedFiles.contains(fileId)) {
                    loS.release(fileId);
                    releasedFiles.remove(fileId);
                    //				numberOfEnqueuedBlocksPerFlush.remove(fileId);

                    if (isTimes) {
                        String[] splitIdPath = blockId.split("_");
                        System.out.println("-- " + splitIdPath[0]);

                        NodeMetadata nm = diS.getMetadataByPathId(splitIdPath[0]);
                        System.out.println("name = " + nm.getName());
                        Pair<String, Long> pair = Charon.times.get(diS.getMetadataByPathId(splitIdPath[0]).getPath());
                        File f = new File("testsOutput");
                        if (!f.exists()) {
                            f.mkdirs();
                        }

                        f = new File("testsOutput" + File.separator + pair.getKey());
                        if (!f.exists()) {
                            try {
                                f.createNewFile();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                        try {
                            FileWriter fw = new FileWriter(f, true);
                            fw.append(System.currentTimeMillis() - pair.getValue() + "\n");
                            fw.close();
                        } catch (IOException e) {

                        }

                    }
                }
            }
        } catch (DirectoryServiceException e1) {
            e1.printStackTrace();
        }
    }

    public int cleanMemory(String fileId) {
        //TODO: este metodo so deve ser chamado no ultimo release
        DataStats dt = dataManager.getDataStats(fileId);
        if (dt == null) {
            return 0;
        }
        String block_fileId = null;
        for (int i = 0; i < dt.getNumberOfBlocks(); i++) {
            block_fileId = fileId.concat("_" + (i + 1));
            if (useMemoryCache) {
                memoryManager.delete(block_fileId);
            } else {
                diskManager.sync(block_fileId);
            }
        }
        return 0;

    }

    public void recover() {
        dataManager.recover(diS, queue);
    }

    public DirectoryServiceImpl getDirectoryService() {
        return diS;
    }

    public static String getFileIdFromBlockId(String blockId) {
        return blockId.split("_")[0];
    }

    public static int getBlockNumber(long offset) {
        return (int) ((offset / blockSize) + 1);
    }

    private int getBlockFromBlockId(String blockId) {
        return new Integer(blockId.split("_")[1]);
    }

}
