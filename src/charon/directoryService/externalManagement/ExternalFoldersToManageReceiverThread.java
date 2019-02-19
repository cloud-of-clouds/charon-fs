package charon.directoryService.externalManagement;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import charon.directoryService.DirectoryServiceImpl;
import charon.directoryService.NameSpace;
import charon.directoryService.NodeMetadata;
import charon.directoryService.NodeType;
import charon.directoryService.exceptions.DirectoryServiceException;
import charon.general.Charon;
import charon.general.CharonConstants;

public class ExternalFoldersToManageReceiverThread extends Thread {

	private DirectoryServiceImpl diS;

	private ExternalManegementThread thread;

	private boolean alive;

	public static final int ADD = 0;
	public static final int DEL = 1;

	public ExternalFoldersToManageReceiverThread(ExternalManegementThread thread, DirectoryServiceImpl diS) {
		this.alive = true;
		this.thread = thread;
		this.diS = diS;
	}


	@Override
	public void run() {

		try {
			ServerSocket server = new ServerSocket(CharonConstants.ADD_EXTERNAL_PORT);
			while(alive){
				Socket clientSocket = server.accept();
				ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream());
				int op = in.readInt();
				switch (op) {
				case ADD:
					String externalFolder = in.readUTF();
					String internalFolder = in.readUTF();
					//					in.close();
					ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
					if(thread.containsFolder(externalFolder)){
						out.writeUTF("Error: external resource is already managed.");
						out.close();
						continue;
					}
					try {
						diS.getMetadata(internalFolder);
						//already exists
						out.writeUTF("Error: there is alread a node with the given name in Charon.");
						out.close();
						continue;
					} catch (DirectoryServiceException e) {}

					String[] divPath = dividePath(internalFolder);
					if(divPath == null){
						out.writeUTF("Error: there is alread a node with the given name in Charon.");
						out.close();
						continue;
					}

					NameSpace ns = diS.getNS(divPath[0]);
					if(ns == null){
						out.writeUTF("Error: "+divPath[0]+" does not exist.");
						out.close();
						continue;
					}
						
					
					NodeMetadata m = NodeMetadata.getDefaultNodeMetadata(divPath[0], divPath[1], NodeType.DIR, System.currentTimeMillis(), Charon.getNextIdPath(ns), externalFolder);
					thread.addFolderToWatch(externalFolder, m);
					diS.putMetadata(m, ns);
					out.writeUTF("OK!");
					out.close();
					break;
				case DEL:
					String folder = in.readUTF();
					thread.remove(folder);
					in.close();
					break;
				default:
					break;
				}
			}
			server.close();
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}


	public void kill() {
		alive=false;
	}


	private String[] dividePath(String path) {

		if(path.equals("/"))
			return null;

		if(!path.startsWith("/"))
			path = "/" + path;
		
		String[] toRet = new String[2];
		String[] split = path.split("/");
		toRet[1] = split[split.length-1];
		if(split.length == 2)
			toRet[0] = path.substring(0, path.length()-toRet[1].length());
		else
			toRet[0] = path.substring(0, path.length()-toRet[1].length()-1);
		return toRet;
	}
}
