package charon.util.compress;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;


public class DeflaterCompressionUtil {

	public static byte[] compress(byte[] array) {
		try {
			Deflater deflater = new Deflater(Deflater.HUFFMAN_ONLY); 
			deflater.setInput(array);  

			ByteArrayOutputStream outputStream = new ByteArrayOutputStream(array.length);   

			deflater.finish();  
			byte[] buffer = new byte[1024];   
			while (!deflater.finished()) {  
				int count = deflater.deflate(buffer); // returns the generated code... index  
				outputStream.write(buffer, 0, count);   
			}  
			outputStream.close();
			byte[] output = outputStream.toByteArray();  

			deflater.end();

			return output;
		} catch (IOException e) {
			e.printStackTrace();
		}  
		return null;
	}

	public static byte[] decompress(byte[] array) {
		try {
			Inflater inflater = new Inflater();   
			inflater.setInput(array);  

			ByteArrayOutputStream outputStream = new ByteArrayOutputStream(array.length);  
			byte[] buffer = new byte[1024];  
			while (!inflater.finished()) {  
				int count;
				count = inflater.inflate(buffer);
				outputStream.write(buffer, 0, count);  
			}  
			outputStream.close();  
			byte[] output = outputStream.toByteArray();  

			inflater.end();

			return output;  
		} catch (DataFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}  
		return null;
	}

}