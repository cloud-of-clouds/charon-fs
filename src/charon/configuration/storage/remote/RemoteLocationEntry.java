package charon.configuration.storage.remote;

import java.io.Externalizable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.text.ParseException;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

import depsky.util.Pair;
import java.util.Objects;

public class RemoteLocationEntry implements Externalizable {

    private String name;
    private int id;
    private String ip;
    private int port;
    private String email;
    private LinkedList<Pair<String, String[]>> cannonicalIds;

    public RemoteLocationEntry() {
        // to EXTERNALIZABLE
    }

    public RemoteLocationEntry(String name, int id, String ip, int port) {
        this.id = id;
        this.ip = ip;
        this.port = port;
        this.name = name;
    }

    public int getPort() {
        return port;
    }

    public String getName() {
        return name;
    }

    public String getIp() {
        return ip;
    }

    public int getId() {
        return id;
    }

    public String getEmail() {
        return email;
    }

    @Override
    public String toString() {
        return name+":"+id + ":" + ip + ":" + port + "| can ->" + (cannonicalIds == null ? "Fail!" : "OK!!");
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + Objects.hashCode(this.name);
        hash = 31 * hash + this.id;
        hash = 31 * hash + Objects.hashCode(this.ip);
        hash = 31 * hash + this.port;
        hash = 31 * hash + Objects.hashCode(this.email);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final RemoteLocationEntry other = (RemoteLocationEntry) obj;
        if (this.id != other.id) {
            return false;
        }
        if (this.port != other.port) {
            return false;
        }
        if (!Objects.equals(this.name, other.name)) {
            return false;
        }
        if (!Objects.equals(this.ip, other.ip)) {
            return false;
        }
        if (!Objects.equals(this.email, other.email)) {
            return false;
        }
        return true;
    }


    public void setEmail(String email) {
        this.email = email;
    }

    public void setCannonicalIds(LinkedList<Pair<String, String[]>> cannonicalIds) {
            this.cannonicalIds = cannonicalIds;
    }

    public boolean isRemoteButUseTheSameAccounts() {
        return cannonicalIds.isEmpty();
    }

    public LinkedList<Pair<String, String[]>> getCannonicalIds() {
        return cannonicalIds;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        this.name = in.readUTF();
        this.id = in.readInt();

        int aux = in.readInt();
        if (aux != -1) {
            this.ip = in.readUTF();
        }

        this.port = in.readInt();
        aux = in.readInt();
        if (aux != -1) {
            this.email = in.readUTF();
        }

        aux = in.readInt();
        if (aux != -1) {
            cannonicalIds = new LinkedList<Pair<String, String[]>>();
            for (int i = 0; i < aux; i++) {
                Pair<String, String[]> p = new Pair<String, String[]>();
                p.readExternal(in);
                cannonicalIds.add(p);
            }
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(name);
        
        out.writeInt(id);
        if (ip == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(0);
            out.writeUTF(ip);
        }
        out.writeInt(port);
        if (email == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(0);
            out.writeUTF(email);
        }

        if (cannonicalIds == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(cannonicalIds.size());
            for (Pair<String, String[]> p : cannonicalIds) {
                p.writeExternal(out);
            }
        }

        out.flush();
    }

    public static List<String[]> readCannonicalIds(int clientId, File credentialsFile) throws ParseException {
        try {
            Scanner sc = new Scanner(credentialsFile);
            String line;
            String[] splitLine;
            int lineNum = -1;
            boolean flag = false;
            LinkedList<String[]> canonicalIds = new LinkedList<String[]>();
            String[] pair = new String[2];
            int cont = 0;
            int numOfIds = 0;
            while (sc.hasNext()) {
                lineNum++;
                line = sc.nextLine();
                if (line.startsWith("#") || line.equals("")) {
                    continue;
                }

                splitLine = line.split("=", 2);

                if (splitLine.length != 2) {
                    sc.close();
                    throw new ParseException("Bad formated credentials.charon file.", lineNum);
                }

                if ((splitLine[0].equals("id") && (new Integer(splitLine[1])) == clientId) || flag) {
                    if (flag == false) {
                        flag = true;
                    }
                    if (splitLine[0].equals("id")) {
                        numOfIds++;
                    }

                    if (numOfIds == 1) {
//                        if (splitLine[0].equals("email")) {
//                            pair[0] = "email";
//                            pair[1] = splitLine[1];
//                            canonicalIds.add(pair);
//                            pair = new String[2];
//                        } else 
                            if (splitLine[0].equals("driver.type")) {
                            pair[0] = splitLine[1];
                        } else if (splitLine[0].equals("canonicalId")) {
                            if (pair[0].equals("WINDOWS-AZURE")) {
                                pair[1] = "";
                            } else {
                                pair[1] = splitLine[1];
                            }
                            canonicalIds.add(pair);
                            pair = new String[2];
                            cont++;
                        }
                        if (cont == 4) {
                            break;
                        }
                    }
                }
            }
            sc.close();

            //SECOND PART.
            return canonicalIds;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

}
