package charon.util;

import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class IOUtil {

	private static final byte[] HEX_CHAR_TABLE = {
		(byte) '0', (byte) '1', (byte) '2', (byte) '3',
		(byte) '4', (byte) '5', (byte) '6', (byte) '7',
		(byte) '8', (byte) '9', (byte) 'a', (byte) 'b',
		(byte) 'c', (byte) 'd', (byte) 'e', (byte) 'f'
	};

	public static String getHexString(byte[] raw) {

		if(raw==null)
			return "null";		

		byte[] hex = new byte[2 * raw.length];
		int index = 0;
		for (byte b : raw) {
			int v = b & 0xFF;
			hex[index++] = HEX_CHAR_TABLE[v >>> 4];
			hex[index++] = HEX_CHAR_TABLE[v & 0xF];
		}
		String res;
		try {
			res = new String(hex, "ASCII");
		} catch (UnsupportedEncodingException e) {
			return "UnsupportedEncodingException getHexString"; 
		}


		return res;
	}

	public static void readFromOIS(ObjectInput ois , byte[] toFill) throws IOException{
		for(int i = 0 ; i< toFill.length ; i++)
			toFill[i] = ois.readByte();
	}

	public static void writeToOOS(ObjectOutput out , byte[] towrite) throws IOException{
		out.write(towrite);
	}

	public static byte[] generateSHA1Hash(byte[] data){
		try {
			MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
			return sha1.digest(data);

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static void closeStream(Closeable stream){
		try {
			stream.close();
		} catch (IOException e) {
			//ignore close execption.
		}
	}


}
