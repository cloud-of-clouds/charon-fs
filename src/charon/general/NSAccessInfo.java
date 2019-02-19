package charon.general;

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import charon.configuration.Location;
import com.amazonaws.util.json.JSONArray;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;
import depsky.util.Pair;
import java.util.ArrayList;
import java.util.Iterator;

public class NSAccessInfo implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = -2161540724683816008L;
    private List<Pair<String, String[]>> credsToShare;
    private String[] credsToTheCaseOfAmazon;
    private int owner;
    private int[] peers;
    private String[] permissions;
    private Location location;
    private boolean usingTheSameAccountsAsOwner = false;

    public NSAccessInfo(List<Pair<String, String[]>> credsToShare, int owner, Location location) {
        this(credsToShare, owner, new int[]{owner}, new String[]{"rw"}, location);
    }

    public NSAccessInfo(List<Pair<String, String[]>> credsToShare, int owner, int[] peers, String[] permissions, Location location) {
        if (credsToShare == null) {
            this.credsToShare = null;
            this.usingTheSameAccountsAsOwner = true;
        } else {
            this.credsToShare = credsToShare;
        }
        this.owner = owner;
        this.peers = peers;
        this.location = location;
        this.permissions = permissions;

        switch (this.location) {
            case SINGLE_CLOUD:
                if (credsToShare == null) {
                    break;
                }
                for (Pair<String, String[]> cannonicals : credsToShare) {
                    if (cannonicals.getKey().equals("AMAZON-S3")) {
                        credsToTheCaseOfAmazon = cannonicals.getValue();
                    }
                }
                break;
            default:
                break;
        }

    }

    public NSAccessInfo(JSONObject jsonSrc) throws JSONException {
        JSONObject json = jsonSrc.getJSONObject("NS-Info");

        this.owner = json.getInt("owner");
        this.usingTheSameAccountsAsOwner = json.getBoolean("usingSameAccounts");
        this.location = Location.valueOf(json.getString("location"));

        //peers
        JSONArray jarray = json.getJSONArray("peers");
        this.peers = new int[jarray.length()];
        for (int i = 0; i < peers.length; i++) {
            peers[i] = jarray.getInt(i);
        }

        //permissions
        jarray = json.getJSONArray("permissions");
        this.permissions = new String[jarray.length()];
        for (int i = 0; i < permissions.length; i++) {
            permissions[i] = jarray.getString(i);
        }

        //aws-only-sharing-credentials
        if (location == Location.SINGLE_CLOUD && !usingTheSameAccountsAsOwner) {
            jarray = json.getJSONArray("aws-only-sharing-credentials");
            this.credsToTheCaseOfAmazon = new String[jarray.length()];
            for (int i = 0; i < credsToTheCaseOfAmazon.length; i++) {
                credsToTheCaseOfAmazon[i] = jarray.getString(i);
            }
        } else {
            credsToTheCaseOfAmazon = null;
        }

        //sharing-credentials
        if (!usingTheSameAccountsAsOwner) {
            this.credsToShare = new ArrayList<>();
            jarray = json.getJSONArray("sharing-credentials");
            for (int i = 0; i < jarray.length(); i++) {
                JSONObject jsonObj = jarray.getJSONObject(i);
                for (Iterator<String> iterator = jsonObj.keys(); iterator.hasNext();) {
                    String key = iterator.next();
                    JSONArray jsonArray = jsonObj.getJSONArray(key);
                    String[] strArray = new String[jsonArray.length()];
                    for (int j = 0; j < strArray.length; j++) {
                        strArray[j] = jsonArray.getString(j);
                    }
                    credsToShare.add(new Pair<String, String[]>(key, strArray));
                }
            }
        } else {
            credsToShare = null;
        }
    }

    public List<Pair<String, String[]>> getCredToAccessSNSOwnedByOthers() {
        return credsToShare;
    }

    public LinkedList<Pair<String, String[]>> getCredToAccessSNSOwnedByMe() {
        LinkedList<Pair<String, String[]>> toRet = new LinkedList<Pair<String, String[]>>();
        for (Pair<String, String[]> pair : credsToShare) {
            if (pair.getKey().equals("RACKSPACE") || pair.getKey().equals("WINDOWS-AZURE")) {
                toRet.add(new Pair<String, String[]>(pair.getKey(), null));
            } else {
                toRet.add(pair);
            }
        }
        return toRet;
    }

    public String[] getCredToAccessSNSOnAmazon() {
        return credsToTheCaseOfAmazon;
    }

    public int getOwnerId() {
        return this.owner;
    }

    public boolean isNSPrivate() {
        return credsToShare == null;
    }

    public int[] getPeers() {
        return peers;
    }

    public void setPermissions(int id, String permissions) {
        for (int i = 0; i < peers.length; i++) {
            if (peers[i] == id) {
                this.permissions[i] = permissions;
            }
        }
    }

    public void addPeer(int id, String permissions) {
        for (int i : peers) {
            if (i == id) {
                return;
            }
        }

        peers = Arrays.copyOf(peers, peers.length + 1);
        peers[peers.length - 1] = id;
        this.permissions = Arrays.copyOf(this.permissions, this.permissions.length + 1);
        this.permissions[this.permissions.length - 1] = permissions;
    }

    public String getPermissions(int id) {
        for (int i = 0; i < peers.length; i++) {
            if (peers[i] == id) {
                return this.permissions[i];
            }
        }
        return null;
    }

    public boolean getUsingTheSameAccountsAsOwner() {
        return this.usingTheSameAccountsAsOwner;
    }

    public JSONObject toJson() throws JSONException {

        JSONObject json = new JSONObject();

        json.put("location", location.toString());
        json.put("owner", owner);
        json.put("usingSameAccounts", usingTheSameAccountsAsOwner);
        json.put("peers", new JSONArray(peers));
        json.put("permissions", new JSONArray(permissions));
        json.put("aws-only-sharing-credentials", location == Location.SINGLE_CLOUD && !usingTheSameAccountsAsOwner ? new JSONArray(credsToTheCaseOfAmazon) : "null");

        if (!usingTheSameAccountsAsOwner) {
            JSONArray jArray = new JSONArray();
            for (Pair<String, String[]> pair : credsToShare) {
                jArray.put(new JSONObject().put(pair.getKey(), new JSONArray(pair.getValue())));
            }

            json.put("sharing-credentials", jArray);
        } else {
            json.put("sharing-credentials", "null");
        }

        return new JSONObject().put("NS-Info", json);
    }

}
