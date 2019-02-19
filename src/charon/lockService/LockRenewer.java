package charon.lockService;

import charon.general.Printer;

public class LockRenewer extends Thread {

	private boolean kill;
	private int time;
	private LockService lockService;
	private String pathId;
	private int percent;

	public LockRenewer(int time, LockService lockService, String pathId) {
		this.kill = false;
		this.time = time;
		this.lockService = lockService;
		this.pathId = pathId;
		this.percent = (int) (time * LockService.ERROR_PERCENT);
	}

	@Override
	public void run() {
		boolean exception = false;
		int renewError = 0;
		while(!kill){
			System.out.println("NEW RENEWER THREAD - Renewer task KILL? - " + kill);
			try {
				if(!exception && renewError == 0)
					Thread.sleep(time-percent);
				
				Printer.println("- Renewer Thread.", "roxo");
				if(!kill && lockService.renew(pathId, time)){
					renewError = 0;
				}else{
					renewError ++;
					if(renewError == 4)
						kill=true;
				}
				exception = false;
			} catch (InterruptedException e) {
				exception = true;
			} 
		}
		System.out.println("RenewTask Finish! > " + pathId);
	}

	public void exit() {
		System.out.println("KILL RENEWER WAS CALLED!");
		kill=true;
	}

}
