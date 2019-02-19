package charon.util;



import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import depsky.client.messages.metadata.ExternalMetadata;

public class ExternalMetadataDummy extends ExternalMetadata{

	public ExternalMetadataDummy() {
		super();
	}
	
	public ExternalMetadataDummy(String wholeDataHash) {
		super(0, true, true, null, wholeDataHash, 0);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		int cont = in.readInt();
		if(cont==-1){
			this.setDataHash(null);
		}else{
			this.setDataHash(in.readUTF());
		}
	}
	
	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		if(this.getWholeDataHash() == null){
			out.writeInt(-1);
		}else{
			out.writeInt(0);
			out.writeUTF(this.getWholeDataHash());
		}
			
	};
}
