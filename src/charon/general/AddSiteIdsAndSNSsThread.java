package charon.general;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;

import charon.configuration.storage.remote.RemoteLocationEntry;
import charon.directoryService.DirectoryServiceImpl;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;
import depsky.util.Pair;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.FileUtils;

public class AddSiteIdsAndSNSsThread extends Thread {

    private DirectoryServiceImpl diS;
    private String siteIdsDir;
    private String sharedTokensDir;
    private String fileWithAddedGrantees = "config/addedGrantees";

    public AddSiteIdsAndSNSsThread(DirectoryServiceImpl diS, String sharedTokensDir, String siteIdsDir) {
        this.diS = diS;
        this.sharedTokensDir = sharedTokensDir;
        this.siteIdsDir = siteIdsDir;
        File folder = new File(sharedTokensDir);
        if (!folder.exists()) {
            while (!folder.mkdirs());
        }

        folder = new File(siteIdsDir);
        if (!folder.exists()) {
            while (!folder.mkdirs());
        }

        File file = new File(fileWithAddedGrantees);
        try {
            if (!file.exists()) {
                while (!file.createNewFile());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        while (true) {
            File folder = new File(sharedTokensDir);
            String[] listOfFiles;
            listOfFiles = folder.list();
            for (int i = 0; i < listOfFiles.length; i++) {
                File f = new File(sharedTokensDir + File.separator + listOfFiles[i]);

                try {
//                    ObjectInputStream ois = new ObjectInputStream(new FileInputStream(f));
//                    String idPath = ois.readUTF();
//                    NSAccessInfo accInfo = (NSAccessInfo) ois.readObject();

                    String jsonSrc = FileUtils.readFileToString(f);
                    JSONObject json = new JSONObject(jsonSrc);
                    String idPath = json.getString("NS-id");
                    NSAccessInfo accInfo = new NSAccessInfo(json);

                    System.out.println("-> adding new SNS");
                    if (!accInfo.getUsingTheSameAccountsAsOwner()) {
                        for (Pair<String, String[]> asd : accInfo.getCredToAccessSNSOwnedByOthers()) {
                            for (String str : asd.getValue()) {
                                System.out.println(asd.getKey() + " - " + str);
                            }
                        }
                    } else {
                        System.out.println("Using the same accounts as the owner");
                    }
                    if (idPath != null && !idPath.equals("")) {
                        diS.addNS(idPath, accInfo);
                    }
//                    ois.close();
                    f.delete();
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    System.out.println(e.getLocalizedMessage() + ": AddNewSNSs");
                    f.delete();
                } catch (JSONException ex) {
                    ex.printStackTrace();
                }
            }

            folder = new File(siteIdsDir);
            listOfFiles = folder.list();
            for (int i = 0; i < listOfFiles.length; i++) {
                File f = new File(siteIdsDir + File.separator + listOfFiles[i]);
                //REMOTE CONFIGURATION.
                List<RemoteLocationEntry> remoteLocationEntries = null;
                try {
                    remoteLocationEntries = readRemoteLocationEntries(f);
                } catch (FileNotFoundException e) {
                    //IT WILL NEVER HAPPEN.
                    remoteLocationEntries = new ArrayList<RemoteLocationEntry>();
                }
                for (RemoteLocationEntry rle : remoteLocationEntries) {
                    System.out.println("-> Adding SiteID.. | " + rle.getId() + " - " + rle.getIp() + ":" + rle.getPort());
                    try {

                        //CanonicalIds
                        LinkedList<Pair<String, String[]>> cannonicalIds = getSetACLFormatCannonicalIds(rle.getId(), f);
                        rle.setCannonicalIds(cannonicalIds);

                    } catch (ParseException e) {
                        System.out.println("(-) Site ID File () bad format.");
                    }
                    PrintWriter out = null;
                    BufferedWriter bw = null;
                    FileWriter fw = null;
                    try {
                        fw = new FileWriter(fileWithAddedGrantees, true);
                        bw = new BufferedWriter(fw);
                        out = new PrintWriter(bw);
                        out.println("(" + rle.getId() + ")" + rle.getName() + "-" + rle.getIp() + ":" + rle.getPort());
                        out.close();
                        bw.close();
                        fw.close();
                    } catch (IOException e) {
                        // File writing/opening failed at some stage.
                        System.out.println("ERROR WRITING IN THE ADDED GRANTEES FILE.");
                    }

                    diS.addRemotePeer(rle);
                }

                //EMAILS CONFIGURATION.
                try {
                    Map<Integer, String> emails = readEmails(f);
                    for (java.util.Map.Entry<Integer, String> e : emails.entrySet()) {
                        diS.putEmail(e.getKey(), e.getValue());
                    }

                } catch (FileNotFoundException e) {
                    //IT WILL NEVER HAPPEN.
                    System.out.println("(-) Site ID File () does not exist.");
                }

                f.delete();
            }

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
            }

        }
    }

    private Map<Integer, String> readEmails(File credentialsFile) throws FileNotFoundException {
        Scanner sc = new Scanner(credentialsFile);
        Map<Integer, String> res = new ConcurrentHashMap<Integer, String>();
        String line;
        int currentID = -1;
        String currentEmail = null;
        while (sc.hasNext()) {
            line = sc.nextLine();
            if (line.startsWith("#") || line.equals("")) {
                continue;
            }
            if (line.startsWith("id=")) {
                currentID = Integer.parseInt(line.split("=")[1]);
            }
            if (line.startsWith("email=")) {
                currentEmail = line.split("=", 2)[1];
                if (!currentEmail.contains("@") || currentID == -1) {
                    currentEmail = null;
                    currentID = -1;
                    continue;
                }
                res.put(currentID, currentEmail);
            }
        }
        sc.close();
        return res;
    }

    private List<RemoteLocationEntry> readRemoteLocationEntries(File credentialsFile) throws FileNotFoundException {
        Scanner sc = new Scanner(credentialsFile);
        List<RemoteLocationEntry> res = new ArrayList<RemoteLocationEntry>();
        String line;
        int currentID = -1;
        String currentAddr = null;
        String currentName = null;
        while (sc.hasNext()) {
            line = sc.nextLine();
            if (line.startsWith("#") || line.equals("")) {
                continue;
            }
            if (line.startsWith("name=")) {
                currentName = line.split("=")[1];
            }
            if (line.startsWith("id=")) {
                currentID = Integer.parseInt(line.split("=")[1]);
            }
            if (line.startsWith("addr=")) {
                currentAddr = line.split("=", 2)[1];
                if (!currentAddr.contains(":") || currentID == -1) {
                    currentAddr = null;
                    currentID = -1;
                    continue;
                }
                res.add(new RemoteLocationEntry(currentName, currentID, currentAddr.split(":", 2)[0], Integer.parseInt(currentAddr.split(":", 2)[1])));
            }
        }
        sc.close();
        return res;
    }

    private LinkedList<Pair<String, String[]>> getSetACLFormatCannonicalIds(int id, File credentials) throws ParseException {
        String[] aux = null;
        LinkedList<Pair<String, String[]>> cannonicalIds = new LinkedList<Pair<String, String[]>>();

        List<String[]> l = RemoteLocationEntry.readCannonicalIds(id, credentials);

        //o site-id do cliente 'id' não trás informação dos cannonical ids, o que significa que usa as mesmas contas que este cliente
        for (String[] s : l) {
            if (s[0].equals("RACKSPACE")) {
                aux = new String[2];
                aux[0] = s[1].split(",")[0];
                aux[1] = s[1].split(",")[1];
                cannonicalIds.add(new Pair<String, String[]>(s[0], aux));
            } else {
                aux = new String[1];
                aux[0] = s[1];
                cannonicalIds.add(new Pair<String, String[]>(s[0], aux));
            }
        }
        return cannonicalIds;
    }
}
