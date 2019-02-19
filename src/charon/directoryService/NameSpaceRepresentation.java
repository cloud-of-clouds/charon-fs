package charon.directoryService;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import charon.util.IOUtil;

public class NameSpaceRepresentation implements Externalizable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 3789101906604929333L;

	private long versionNum;
	private String hash;
	private byte[] serializedNS;

	public NameSpaceRepresentation( long versionNum, String hash, byte[] serializedNS) {
		this.versionNum = versionNum;
		this.hash = hash;
		this.serializedNS = serializedNS;
	}

	public NameSpaceRepresentation() {
	}

	public String getHash() {
		return hash;
	}

	public byte[] getSerializedNS() {
		return serializedNS;
	}

	public long getVersionNum() {
		return versionNum;
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
	ClassNotFoundException {
		versionNum = in.readLong();
		//		System.out.println(" -- read vNumber = " + versionNum);
		int count = in.readInt();

		//		System.out.println("++ sizeHash = " + count);
		if(count!=-1){
			hash = in.readUTF();
		}

		count = in.readInt();
		//		System.out.println("++ sizeNS = " + count);
		if(count!=-1){
			serializedNS = new byte[count];
			IOUtil.readFromOIS(in, serializedNS);
		}
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeLong(versionNum);
		//		System.out.println("++ vNumber = " + versionNum );

		//		System.out.println("++ sizeHash = " + hash==null ? -1 : hash.length);
		out.writeInt(hash==null ? -1 : 0);
		if(hash!=null)
			out.writeUTF(hash);

		//		System.out.println("++ sizeNS = " + serializedNS==null ? -1 : serializedNS.length);
		out.writeInt(serializedNS==null ? -1 : serializedNS.length);
		if(serializedNS != null)
			IOUtil.writeToOOS(out, serializedNS);

		out.flush();
	}

}
