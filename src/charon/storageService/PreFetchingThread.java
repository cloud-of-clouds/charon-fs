package charon.storageService;

import java.util.concurrent.LinkedBlockingQueue;

import charon.configuration.CharonConfiguration;
import charon.configuration.Location;
import charon.configuration.storage.cloud.SingleCloudConfiguration;
import charon.directoryService.DirectoryServiceImpl;
import charon.storageService.accessors.AmazonAcessor;
import charon.storageService.accessors.DepSkyAcessor;
import charon.storageService.accessors.PrivateRepositoryAcessor;
import depsky.client.messages.metadata.ExternalMetadata;
import depsky.util.Pair;

public class PreFetchingThread extends Thread {

    private DepSkyAcessor depsky;
    private StorageService DaS;
    private LinkedBlockingQueue<Pair<String, Pair<Location, ExternalMetadata>>> jobs;
    private AmazonAcessor amazon;
    private PrivateRepositoryAcessor localRep;

    public PreFetchingThread(int clientId, CharonConfiguration config, StorageService DaS, DirectoryServiceImpl diS,
            LinkedBlockingQueue<Pair<String, Pair<Location, ExternalMetadata>>> jobs) {
        this.jobs = jobs;

        if (config.getCoCConfiguration() != null) {
            this.depsky = DepSkyAcessor.getInstance(clientId, config, config.getCoCConfiguration(), diS);
        }
        this.DaS = DaS;

        SingleCloudConfiguration cred = config.getSingleCloudConfig();
        if (cred != null) {
            this.amazon = new AmazonAcessor(clientId, cred.getAccessKey(), cred.getSecretKey(), config, diS);
        }
        if (config.getPrivateRepConfig() != null) {
            this.localRep = new PrivateRepositoryAcessor(config, diS);
        }
    }

    public void run() {
        while (true) {

            Pair<String, Pair<Location, ExternalMetadata>> job;
            String fileId;
            try {
                job = jobs.take();

                System.out.println(Thread.currentThread().getId() + " ---- INIT FETCHING BLOCK: " + job.getKey());
                byte[] valueRead = null;
                switch (job.getValue().getKey()) {
                    case CoC:
                        if (job.getValue().getValue() == null) {
                            DaS.readErrorOnPrefetching(job.getKey());
                        } else {
                            valueRead = depsky.readMatchingFromDepSky(job.getKey(), job.getValue().getValue());
                        }
                        break;
                    case SINGLE_CLOUD:
                        if (job.getValue().getValue() == null) {
                            DaS.readErrorOnPrefetching(job.getKey());
                        } else if (amazon != null) {
                            valueRead = amazon.readMatchingFrom(job.getKey(), job.getValue().getValue().getWholeDataHash());
                        }
                        break;
                    case PRIVATE_REP:
                        if (job.getValue().getValue() == null) {
                            DaS.readErrorOnPrefetching(job.getKey());
                        } else {
                            valueRead = localRep.read(job.getKey(), job.getValue().getValue().getWholeDataHash());
                        }
                        //						valueRead = localRep.readFromLocalRep(block_fileId, versionInfo.getWholeDataHash(), diS.getSNSAccessInfo(split[0]).getPeers());

                        break;
//				case EXTERNAL_REP:
//					if(job.getValue().getValue() == null)
//						DaS.readErrorOnPrefetching(job.getKey());
//					else
//						valueRead = externalRep.read(job.getKey(), job.getValue().getValue().getWholeDataHash());
//					//						valueRead = localRep.readFromLocalRep(block_fileId, versionInfo.getWholeDataHash(), diS.getSNSAccessInfo(split[0]).getPeers());
//
//					break;
                    default:
                        break;
                }
                if (valueRead == null) {
                    DaS.readErrorOnPrefetching(job.getKey());
                } else {
                    DaS.updataCacheFromPreFetching(job.getKey(), job.getValue().getValue(), valueRead);
                }
            } catch (InterruptedException e) {
                //e.printStackTrace();
            }

        }
    }
}
