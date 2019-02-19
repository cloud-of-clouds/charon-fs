package charon.general;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import charon.directoryService.DirectoryServiceImpl;
import charon.directoryService.NameSpace;
import charon.directoryService.NameSpacesUpdater;
import charon.lockService.LockRenewer;
import charon.lockService.LockService;
import charon.storageService.accessors.DepSkyAcessor;
import charon.storageService.accessors.IMetadataAccessor;

public class LockUpdterNSSwitcher implements LockService {
    
    private IMetadataAccessor accessor;
    private Map<String, Integer> lockedNs;
    private Map<String, LockRenewer> renewerTasks;
    private Map<String, Long> lastTimeLocked;
    private NameSpacesUpdater nsUpdater;
    
    
    public LockUpdterNSSwitcher() {
        this.lockedNs = new ConcurrentHashMap<String, Integer>();
        this.renewerTasks = new ConcurrentHashMap<String, LockRenewer>();
        this.lastTimeLocked = new ConcurrentHashMap<String, Long>();
    }
    
    public void initNSUpdater(DirectoryServiceImpl dis, IMetadataAccessor acessor){
        this.accessor = acessor;
        nsUpdater = new NameSpacesUpdater(dis, accessor, this);
        nsUpdater.start();
    }
    
    @Override
    public boolean tryAcquire(String pathId, int time) {
        
        String NS = pathId.split("#",2)[0];
        if(accessor==null)
            return true;
        
        synchronized(NS.intern()){
            if(lockedNs.containsKey(NS)){
                lockedNs.put(NS, lockedNs.get(NS)+1);
                //			System.out.println("TRYAQUIRE!! NS COUNTER - lockedNs.get(NS) = " + lockedNs.get(NS));
                if(!renewerTasks.containsKey(NS)){
                    LockRenewer renewTask = new LockRenewer(time, this, NS);
                    renewerTasks.put(NS, renewTask);
                    renewTask.start();
                }
                return true;
            }else{
                if(justLock(pathId, time)){
                    lockedNs.put(NS, 1);
                    LockRenewer renewTask = new LockRenewer(time, this, NS);
                    renewerTasks.put(NS, renewTask);
                    renewTask.start();
                    
                    return true;
                }
                return false;
            }
        }
    }
    
    @Override
    public boolean justLock(String pathId, int time){
        
        String NS = pathId.split("#",2)[0];
        synchronized(NS.intern()){
            if(!lockedNs.containsKey(NS)){
                nsUpdater.stopUpdate(NS);
                lockedNs.put(NS, 0);
                lastTimeLocked.put(NS, System.currentTimeMillis());
                Printer.println(" : Locking " + NS, "vermelho");
                long t = System.currentTimeMillis();
                
                boolean b = accessor.lock(NS, LOCK_TIME); // LOCKING!!!
                long tempo = System.currentTimeMillis()-t;
                if(b){
                    Printer.println(" : Successfull! [ took: " + tempo + " ]", "vermelho");
                    lastTimeLocked.put(NS, System.currentTimeMillis());
                }else{
                    Printer.println(" : Failed! [ took: " + tempo + " ]", "vermelho");
                    nsUpdater.runUpdate(NS);
                    lockedNs.remove(NS);
                }
                
                return b;
            }
            
            if(!renewerTasks.containsKey(NS)){
                if(lastTimeLocked.containsKey(NS)) {
                    if((System.currentTimeMillis() - lastTimeLocked.get(NS)) > (LOCK_TIME -(LOCK_TIME*ERROR_PERCENT))) {
                        Printer.println("- justLock Renew", "roxo");
                        boolean b = renew(NS, time);
                        if(!b)
                            lockedNs.remove(NS);
                        return b;
                    }
                }else{
                    //				Printer.println("\n\n\n JustLOCK: NAO HAVIA LAST TIME LOCKED!!!!!!!!\n\n\n ", "roxo");
                    return false;
                }
            }
        }
        return true;
    }
    
    @Override
    public void release(String pathId) {
        //TODO: pode acontercer o ficheiro ficar aberto, os dados acabarem de ser enviados
        // e a SendingThread chama o release. Que faz unlock com o ficheiro aberto!
        String[] name = pathId.split("#",2);
        String NS = name[0];
        //		System.out.println("release path ID = " + NS);
        
        if(accessor==null)
            return;
        
        synchronized(NS.intern()){
            if(lockedNs.containsKey(NS)){
                lockedNs.put(NS, lockedNs.get(NS)-1);
                //			System.out.println("NS counter = " + lockedNs.get(NS));
                if(lockedNs.get(NS)<=0 && renewerTasks.containsKey(NS)){
                    renewerTasks.remove(NS).exit();
                }
            }
        }
    }
    
    @Override
    public boolean renew(String NS, int time){
        //		Printer.println("-------> RENEW CALLED!", "roxo");
        if(accessor==null)
            return true;
        
        if(accessor.lock(NS, LOCK_TIME)){
            this.lastTimeLocked.put(NS, System.currentTimeMillis());
            return true;
        }else
            return false;
        
    }
    
    
    @Override
    public boolean isLocked(String ns) {
        //		Printer.print("Is the SNS [" + ns +"] locked ? - ", "azul");
        if(!lockedNs.containsKey(ns)){
            //			Printer.println("false1", "azul");
            return false;
        } else {
            if(!renewerTasks.containsKey(ns)){
                if(lastTimeLocked.containsKey(ns)){
                    if((System.currentTimeMillis() - lastTimeLocked.get(ns)) > (LOCK_TIME-(LOCK_TIME*ERROR_PERCENT))){
                        
                        //						Printer.println("false2", "azul");
                        lockedNs.remove(ns);
                        lastTimeLocked.remove(ns);
                        return false;
                    }
                }else{
                    Printer.println("isLocked: NAO HAVIA LAST TIME LOCKED!!!!!!!!", "roxo");
                    return false;
                }
            }
        }
        //		Printer.println("true", "azul");
        return true;
    }
    
    
    public void addNameSpaceToManage(NameSpace ns) {
        nsUpdater.addNameSpace(ns);
    }
    
    public void removeNameSpaceToManage(String nsId) {
        nsUpdater.removeNS(nsId);
    }
    
//	private int getIndexOfCloudConfiguration(String cloudId, List<SingleCloudConfiguration> list){
//		int index = 0;
//		for(SingleCloudConfiguration cloudConf : list){
//			if(cloudConf.getDriverType().equals(cloudId))
//				break;
//			index++;
//		}
//		return index;
//	}
    
}
