package charon.lockService;


public interface LockService {

	public static final int LOCK_TIME = 20000;
	public static final double ERROR_PERCENT = 0.1;
	
	public boolean tryAcquire(String pathId, int time);
	public boolean justLock(String pathId, int time);
	public void release(String pathId);
	public boolean renew(String NS, int time);
	public boolean isLocked(String ns);
	
	
}
