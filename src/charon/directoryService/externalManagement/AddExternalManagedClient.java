package charon.directoryService.externalManagement;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

import charon.general.CharonConstants;

public class AddExternalManagedClient {

	public static void main(String[] args) {

		if(args.length < 2 || !(args[0].equals("ADD") || args.equals("DEL"))){
			System.err.println("usage: java <ADD || DEL> <externalFolder> <folderInCharon>");
			System.exit(0);
		}

		if(args[0].equals("ADD") && args.length < 3){
			System.err.println("usage: java ADD <externalFolder> <folderInCharon>");
			System.exit(0);
		}

		String op = args[0];
		String externalFolder = args[1];
		String internalFolder = null;
		if(args.length > 2)
			internalFolder = args[2];

		Socket clientSocket = null;
		try {
			clientSocket = new Socket("127.0.0.1", CharonConstants.ADD_EXTERNAL_PORT);
		} catch (UnknownHostException e) {
			System.err.println("(-) UnknownHostException 127.0.0.1:" + CharonConstants.ADD_EXTERNAL_PORT);
			System.exit(0);
		} catch (IOException e) {
			System.err.println("(-) IOException:" + e.getMessage());
			System.exit(0);
		}

		try {
			if(op.equals("ADD")){
				ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
				out.writeInt(ExternalFoldersToManageReceiverThread.ADD);
				out.writeUTF(externalFolder);
				out.writeUTF(internalFolder);
				out.flush();
				//					out.close();

				ObjectInputStream ois = new ObjectInputStream(clientSocket.getInputStream());
				String res = ois.readUTF();

				ois.close();
				clientSocket.close();

				System.out.println(res);
			}else if(op.equals("DEL")){
				ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
				out.writeInt(ExternalFoldersToManageReceiverThread.DEL);
				out.writeUTF(externalFolder);
				clientSocket.close();
			}

		} catch (IOException e) {
			System.err.println("(-)Somthing went wrong - IOException:" + e.getMessage());
			System.exit(0);
		}
	}

}
