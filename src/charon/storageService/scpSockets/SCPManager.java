package charon.storageService.scpSockets;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

import charon.configuration.storage.remote.RemoteLocationEntry;


public class SCPManager {

	protected static final int GET_PRIV_REP = 0;
	protected static final int PUT_PRIV_REP = 1;
	protected static final int PUT_EXTERNAL_REP = 2;
	protected static final int GET_EXTERNAL_REP = 3;

	public static void remoteLocalRepWrite(String pathId, byte[] data, String dataHash, RemoteLocationEntry peerConfig) {
		if(peerConfig == null)
			return;
		
		SCPObject obj = new SCPObject(pathId, data, dataHash);

		try {
			Socket clientSocket = new Socket(peerConfig.getIp(), peerConfig.getPort());
			ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
			out.writeInt(PUT_PRIV_REP);
			obj.writeExternal(out);
			out.close();
			clientSocket.close();
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (UnknownHostException e) {
			System.out.println("(-) UnknownHost: " + " [" + peerConfig.getIp()+":"+peerConfig.getPort()+"]");
		} catch (IOException e) {
			System.out.println("(-) Unreachable host: " + " [" + peerConfig.getIp()+":"+peerConfig.getPort()+"]");
		}
		
	}
	
	public static void remoteExternalRepWrite(String fileName, byte[] data, long offset, RemoteLocationEntry peerConfig ) {
		if(peerConfig == null)
			return;
		
		SCPObject obj = new SCPObject(fileName, data, offset);

		try {
			Socket clientSocket = new Socket(peerConfig.getIp(), peerConfig.getPort());
			ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
			out.writeInt(PUT_EXTERNAL_REP);
			obj.writeExternal(out);
			out.close();
			clientSocket.close();
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (UnknownHostException e) {
			System.out.println("(-) UnknownHost: " + " [" + peerConfig.getIp()+":"+peerConfig.getPort()+"]");
		} catch (IOException e) {
			System.out.println("(-) Unreachable host: " + " [" + peerConfig.getIp()+":"+peerConfig.getPort()+"]");
		}
		
	}

	public static byte[] remoteLocalRepRead(String pathId, String dataHash, RemoteLocationEntry peerConfig) {
		if(peerConfig == null)
			return null;
		
		SCPObject obj = new SCPObject(pathId, null, dataHash);

		Socket clientSocket;
		try {
			clientSocket = new Socket(peerConfig.getIp(), peerConfig.getPort());
			ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
			out.writeInt(GET_PRIV_REP);
			out.writeUTF(pathId);
			out.writeUTF(obj.getDataHashHex());
			out.flush();
			//					out.close();

			ObjectInputStream ois = new ObjectInputStream(clientSocket.getInputStream());
			obj.readExternal(ois);

			ois.close();
			clientSocket.close();
			if(obj.getData()!=null){
				return obj.getData();
			}

		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (UnknownHostException e) {
			System.out.println("(-) UnknownHost: " + " [" + peerConfig.getIp()+":"+peerConfig.getPort()+"]");
		} catch (IOException e) {
			System.out.println("(-) Unreachable host: " + " [" + peerConfig.getIp()+":"+peerConfig.getPort()+"]");
		}
		return null;
	}
	
	public static byte[] remoteExternalRepRead(String filename, long offset, String dataHash, String externalManaged, RemoteLocationEntry peerConfig) {
		if(peerConfig == null)
			return null;
		
		SCPObject obj = new SCPObject();

		Socket clientSocket;
		try {
			clientSocket = new Socket(peerConfig.getIp(), peerConfig.getPort());
			ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
			out.writeInt(GET_EXTERNAL_REP);
			out.writeUTF(filename);
			out.writeUTF(dataHash);
			out.writeUTF(externalManaged == null ? "null" : externalManaged);
			out.writeLong(offset);
			out.flush();
			
			//					out.close();

			ObjectInputStream ois = new ObjectInputStream(clientSocket.getInputStream());
			obj.readExternal(ois);

			ois.close();
			clientSocket.close();
			if(obj.getData()!=null){
				return obj.getData();
			}

		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (UnknownHostException e) {
			System.out.println("(-) UnknownHost: " + " [" + peerConfig.getIp()+":"+peerConfig.getPort()+"]");
		} catch (IOException e) {
			System.out.println("(-) Unreachable host: " + " [" + peerConfig.getIp()+":"+peerConfig.getPort()+"]");
		}
		return null;
	}
	
}
