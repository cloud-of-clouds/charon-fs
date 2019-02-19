package charon.general;

import java.io.IOException;

public class ShutDownThread extends Thread {
	private String mountPoint;
	private Thread thread;
	public ShutDownThread(String mountPoint, Thread thread) {
		this.thread = thread;
		this.mountPoint = mountPoint;
	}

	@SuppressWarnings("deprecation")
	public void run() {
		System.out.println("\n\nCharon ShutDown!!\nPlease authenticate your sudo account to umount the system.\nMake sure you are not using the mountPoint right now.");
		thread.stop();
		try {
			Thread.sleep(1000);
			Runtime.getRuntime().exec("sudo -D Charon_umount umount " + mountPoint).waitFor();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
