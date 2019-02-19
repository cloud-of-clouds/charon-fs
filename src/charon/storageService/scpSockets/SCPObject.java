package charon.storageService.scpSockets;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class SCPObject implements Externalizable {

	private String pathId;
	private byte[] data;
	private String dataHashHex;
	private long destOffSet;

	public SCPObject() {
	}

	public SCPObject(String pathId, byte[] data, String dataHashHex) {
		this.pathId = pathId;
		this.data = data;
		this.dataHashHex = dataHashHex;
		this.destOffSet = 0;
	}

	public SCPObject(String pathId, byte[] data, long destOffset) {
		this.pathId = pathId;
		this.data = data;
		this.destOffSet = destOffset;
		this.dataHashHex = null;
	}

	public long getDestOffSet() {
		return destOffSet;
	}

	public byte[] getData() {
		return data;
	}

	public String getDataHashHex(){
		return dataHashHex;
	}


	public String getPathId() {
		return pathId;
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
	ClassNotFoundException {
		pathId = in.readUTF();
		int len = in.readInt();
		if(len!=-1){
			data = new byte[len];
			for(int i = 0 ; i < len ; i++)
				data[i] = in.readByte();

			len = in.readInt();
			if(len!=-1)
				dataHashHex = in.readUTF();
		}
		destOffSet = in.readLong();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeUTF(pathId);
		if(data==null)
			out.writeInt(-1);
		else{
			out.writeInt(data.length);

			out.write(data);

			if(dataHashHex!=null){
				out.writeInt(0);
				out.writeUTF(dataHashHex);
			}else
				out.writeInt(-1);
		}

		out.writeLong(destOffSet);
		out.flush();

	}

}
