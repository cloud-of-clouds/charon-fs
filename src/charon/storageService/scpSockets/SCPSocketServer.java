package charon.storageService.scpSockets;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import charon.storageService.StorageService;
import charon.storageService.repositories.FileRepository;

public class SCPSocketServer extends Thread {

    private int port;
    private boolean alive;
    private StorageService stS;
    private FileRepository extRep;
    private FileRepository privRep;

    public SCPSocketServer(int port, StorageService stS, FileRepository privRep, FileRepository extRep) {
        this.port = port;
        this.alive = true;
        this.stS = stS;
        this.extRep = extRep;
        this.privRep = privRep;
    }

    @Override
    public void run() {
        try {
            ServerSocket serverSocket = new ServerSocket(port);
            while (alive) {
                Socket clientSocket = serverSocket.accept();

                try {
                    ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream());
                    int op = in.readInt();
                    if (op == SCPManager.PUT_PRIV_REP) {
                        SCPObject obj = new SCPObject();
                        obj.readExternal(in);
                        in.close();
//						stS.writeInCache(obj.getPathId(), obj.getData(), obj.getDataHashHex());

                        if (privRep != null) {
                            privRep.write(obj.getPathId() + "-" + obj.getDataHashHex(), obj.getData());
                        }

                    } else if (op == SCPManager.GET_PRIV_REP) {
                        String pathId = in.readUTF();
                        String dataHashHex = in.readUTF();
//						in.close();

                        ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
                        byte[] data = stS.readCachedOrLocalRepData(pathId, dataHashHex);

                        new SCPObject(pathId, data, dataHashHex).writeExternal(out);
                        out.close();
                    } else if (op == SCPManager.PUT_EXTERNAL_REP) {
                        SCPObject obj = new SCPObject();
                        obj.readExternal(in);
                        in.close();

//						stS.writeInCache(obj.getPathId(), obj.getData(), obj.getDataHashHex());
                        if (extRep != null) {
                            extRep.write(obj.getPathId(), obj.getData(), obj.getDestOffSet());
                        }
                    } else if (op == SCPManager.GET_EXTERNAL_REP) {
                        String fileName = in.readUTF();
                        String dataHashHex = in.readUTF();
                        String externalManaged = in.readUTF();

                        if (externalManaged.equals("null")) {
                            externalManaged = null;
                        }

                        long offset = in.readLong();
//						in.close();

                        ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
                        byte[] data = stS.readCachedOrExternalRepData(fileName, offset, dataHashHex, externalManaged);

                        new SCPObject(fileName, data, offset).writeExternal(out);
                        out.close();
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }

            }
            serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void kill() {
        alive = false;
    }
}
