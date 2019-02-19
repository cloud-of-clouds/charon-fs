package charon.configuration.storage.remote;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

public class RemoteConfiguration implements Externalizable{

	private List<RemoteLocationEntry> entries;

	public RemoteConfiguration() {
		this.entries=new ArrayList<RemoteLocationEntry>();
	}

	public List<RemoteLocationEntry> getRemotePeersConfig() {
		synchronized (entries) {
			return entries;
		}
	}

	public RemoteLocationEntry getPeerConfig(int id) {
		synchronized (entries) {
			for(RemoteLocationEntry e : entries)
				if(e.getId() == id)
					return e;
			return null;
		}
	}

	public RemoteLocationEntry getPeerConfig(String email) {
		synchronized (entries) {
			for(RemoteLocationEntry e : entries)
				if(e.getEmail().equals(email))
					return e;
			return null;
		}
	}

	@Override
	public String toString() {
		synchronized (entries) {
			StringBuilder sb = new StringBuilder();
			for(RemoteLocationEntry e : entries)
				sb = sb.append(e.toString()).append("\n");
			sb = sb.append("}");
			return sb.toString();
		}
	}

	public void addRemotePeer(RemoteLocationEntry entry){
		synchronized (entries) {
			if(entries.contains(entry))
				entries.remove(entry);

			entries.add(entry);
		}
	}

	public void putEmail(Integer id, String email) {
		synchronized (entries) {
			RemoteLocationEntry peer = getPeerConfig(id);
			if(peer!=null)
				peer.setEmail(email);
		}
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		int size = in.readInt();
		for(int i = 0 ; i<size ; i++){
			RemoteLocationEntry rle = new RemoteLocationEntry();
			rle.readExternal(in);
			entries.add(rle);
		}
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeInt(entries.size());
		for(RemoteLocationEntry e : entries)
			e.writeExternal(out);
		
		out.flush();
	}

}
