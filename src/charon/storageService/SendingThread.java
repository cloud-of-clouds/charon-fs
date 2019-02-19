package charon.storageService;

import charon.configuration.CharonConfiguration;
import charon.configuration.storage.cloud.SingleCloudConfiguration;
import charon.general.NSAccessInfo;
import charon.storageService.accessors.AmazonAcessor;
import charon.storageService.accessors.DepSkyAcessor;
import charon.storageService.accessors.PrivateRepositoryAcessor;
import charon.util.ExternalMetadataDummy;
import depsky.client.messages.metadata.ExternalMetadata;

public class SendingThread extends Thread {

    private StorageService daS;
    private DepSkyAcessor depsky;
    private AmazonAcessor amazon;
    private PrivateRepositoryAcessor localRep;
    private SendingQueue queue;
    private ObjectInQueue object;
    private ObjectInQueue nextObject;
    private Object sync;
    private int threadId;

    public SendingThread(StorageService daS, SendingQueue queue, int clientId, int threadId, CharonConfiguration config) {
        this.daS = daS;
        this.queue = queue;

        if (config.getCoCConfiguration() != null) {
            this.depsky = DepSkyAcessor.getInstance(clientId, config, config.getCoCConfiguration(), daS.getDirectoryService());
        }

        if (config.getSingleCloudConfig() != null) {
            this.amazon = new AmazonAcessor(clientId, config.getSingleCloudConfig().getAccessKey(), config.getSingleCloudConfig().getSecretKey(), config, daS.getDirectoryService());
        }

        if (config.getPrivateRepConfig() != null) {
            this.localRep = new PrivateRepositoryAcessor(config, daS.getDirectoryService());
        }
        
        this.sync = new Object();
        this.object = null;
        this.nextObject = null;
        this.threadId = threadId;
    }

    public void run() {

        boolean flag = false;
        while (true) {
            ExternalMetadata hash = null;
            String hashRes = null;
            try {
                if (object != null) {
                    switch (object.getLocation()) {
                        case CoC:
                            if (object.getData() != null) {
                                hash = depsky.writeToDepSky(object.getFileId(), object.getData(), object.getVersionInfo(), !object.getFileId().contains("_"));
                            } else {
                                hash = depsky.writeToDepSky(object.getFileId(), object.getVersionInfo(), !object.getFileId().contains("_"));
                            }

                            System.out.println("Is metadata dummy = " + (hash instanceof ExternalMetadataDummy));
                            break;
                        case SINGLE_CLOUD:
                            if (amazon != null) {
                                hashRes = amazon.writeTo(object.getFileId());
                                if (hashRes != null) {
                                    hash = new ExternalMetadataDummy(hashRes);
                                }
                            } else {
                                System.out.println("Trying to read Data for single cloud but no Single cloud Configuration was found.");
                            }

                            break;
                        case PRIVATE_REP:
                            NSAccessInfo nsInfo = daS.getDirectoryService().getSNSAccessInfo(object.getNSId());
                            if (nsInfo != null) {
                                hashRes = localRep.write(object.getFileId());
                                if (hashRes != null) {
                                    hash = new ExternalMetadataDummy(hashRes);
                                }
                            }

                            break;
                        default:
                            break;
                    }

                    synchronized (sync) {
                        //is namespace send??
                        if (object.getFileId().contains("_") && hash != null) {
                            daS.commit(object.getFileId(), hash, object.getLocation(), object.getFlushId());
                        }

                        if (nextObject == null) {
                            flag = queue.releaseSendingThread(threadId);
                            if (!flag) {
                                object = null;
                            }
                        } else {
                            object = nextObject;
                            nextObject = null;
                        }
                    }
                } else {
                    Thread.sleep(2000);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    public void setObjectToSend(ObjectInQueue object) {
        synchronized (sync) {
            this.object = object;
        }
    }

    public boolean setSameObjectToSend(ObjectInQueue nextObject) {
        synchronized (sync) {
            if (this.object != null && this.object.getFileId().equals(nextObject.getFileId())) {
                this.nextObject = nextObject;
                return true;
            } else {
                return false;
            }
        }
    }
}
