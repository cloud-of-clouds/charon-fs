package charon.util;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.text.ParseException;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Scanner;

import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.GoogleStorageService;
import org.jets3t.service.model.GSBucket;
import org.jets3t.service.model.GSObject;
import org.jets3t.service.security.GSCredentials;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.microsoft.windowsazure.services.blob.client.CloudBlobClient;
import com.microsoft.windowsazure.services.blob.client.CloudBlobContainer;
import com.microsoft.windowsazure.services.blob.client.CloudBlockBlob;
import com.microsoft.windowsazure.services.blob.client.ListBlobItem;
import com.microsoft.windowsazure.services.core.storage.CloudStorageAccount;
import com.microsoft.windowsazure.services.core.storage.StorageException;

public class CleanClouds {

	public static String charonLocationsFileName = "config/locations.config";
	public static String charonSingleCloudFileName = "config/singleCloud.config";
	public static String charonCoCFileName = "config/depsky.config";
	public static String charonConfigFileName = "config/charon.config";
	public static String AWS = "AMAZON-S3";
	public static String GOOGLE = "GOOGLE-STORAGE";
	public static String AZURE = "WINDOWS-AZURE";
	public static String RACKSPACE = "RACKSPACE";

	public static void main(String[] args) {

		try {
			Properties prop = new Properties();
			FileInputStream input = new FileInputStream(charonLocationsFileName);
			prop.load(input);
			boolean use_coc = (prop.getProperty("coc") == null) ? false : true;
			boolean use_cloud = (prop.getProperty("cloud") == null) ? false : true;
			
			input = new FileInputStream(charonConfigFileName);
			prop.load(input);
			String clientId = prop.getProperty("CLIENT_ID");
			
			if(use_coc){
				cleanAWS(clientId);
				cleanGOOGLE(clientId);
				cleanAZURE(clientId);
				cleanRACKSPACE(clientId);
			}else if(use_cloud){
				cleanAWS(clientId);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	public static void cleanAWS(String clientId){
		try {
			String[] awsCreds = readCredentials(AWS);
			if(awsCreds[0].length() == 0){
				String str = System.getenv().get("AWS_ACCESS_KEY_ID");
				if(str!=null)
					awsCreds[0] = str;

				str = System.getenv().get("AWS_SECRET_ACCESS_KEY");
				if(str!=null)
					awsCreds[1] = str;
			}

			String mprops = "accessKey=" + awsCreds[0] + "\r\n"
					+ "secretKey = " + awsCreds[1];
			PropertiesCredentials b = new PropertiesCredentials( new ByteArrayInputStream(mprops.getBytes()));

			AmazonS3 conn = new AmazonS3Client(b);		
			conn.setEndpoint("http://s3.amazonaws.com"); //Para virtual Box funcionar


			for (Bucket bucket : conn.listBuckets()) {

				if(bucket.getName().startsWith("charon-"+clientId)){
					System.out.println("AWS: Deleting bucket " + bucket.getName());
					ObjectListing objectListing = conn.listObjects(new ListObjectsRequest().withBucketName(bucket.getName()));
					for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {

						conn.deleteObject(bucket.getName(), objectSummary.getKey());
						System.out.println(".");
					}
					conn.deleteBucket(bucket.getName());
				}
			}



		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void cleanGOOGLE(String clientId){

		try {
			String[] googleCreds = readCredentials(GOOGLE);

			GSCredentials gsCredentials = new GSCredentials(googleCreds[0], googleCreds[1]);
			GoogleStorageService gsService = new GoogleStorageService(gsCredentials);

			GSBucket[] contList = gsService.listAllBuckets();
			for(GSBucket bucket : contList){
				if(bucket.getName().startsWith("charon-"+clientId)){
					System.out.println("GOOGLE: Deleting bucket " + bucket.getName());
					GSObject[] objects = gsService.listObjects(bucket.getName());
					for(GSObject obj : objects){
						gsService.deleteObject(bucket.getName(), obj.getName());
						System.out.println(".");
					}
					gsService.deleteBucket(bucket.getName());
				}
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (ServiceException e) {
			e.printStackTrace();
		}
	}

	public static void cleanAZURE(String clientId){
		try {
			String[] azureCreds = readCredentials(AZURE);

			String storageConnectionString = 
					"DefaultEndpointsProtocol=https;" + 
							"AccountName=" + azureCreds[0] + ";" + 
							"AccountKey=" + azureCreds[1] + ";";

			CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);
			CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
			Iterable<CloudBlobContainer> list = blobClient.listContainers();
			for(CloudBlobContainer cont : list){
				if(cont.getName().startsWith("charon-"+clientId)){
					System.out.println("AZURE: Deleting bucket " + cont.getName());
					Iterable<ListBlobItem> blobs = cont.listBlobs();
					CloudBlockBlob blob;
					for(ListBlobItem b : blobs){
						String[] name = b.getUri().getPath().split("/");
						blob = cont.getBlockBlobReference(name[name.length-1]);
						blob.deleteIfExists();
						System.out.println(".");
					}
					cont.delete();
				}
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (InvalidKeyException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		} catch (StorageException e) {
			e.printStackTrace();
		}
	}

	public static void cleanRACKSPACE(String clientId){
		try {
			String[] rackspaceCreds = readCredentials(RACKSPACE);

			String accessKey = rackspaceCreds[0];
			String secretKey = rackspaceCreds[1];
			String accessURL = "https://lon.identity.api.rackspacecloud.com/v2.0/";
			String getToken = "tokens";
			String tokenId = null;
			String operationURL = null;

			String content = "{"+
					"\"auth\": {"+
					"\"RAX-KSKEY:apiKeyCredentials\": {"+
					"\"username\": \""+accessKey+"\","+
					"\"apiKey\": \""+ secretKey+ "\"" +
					"}}}";
			CloseableHttpClient client = HttpClients.createDefault();
			//authenticate
			HttpPost post = new HttpPost(accessURL+getToken);
			post.addHeader("Content-Type", "application/json");
			post.addHeader("Accept", "application/json");
			HttpEntity entity;
			entity = new StringEntity(content);
			post.setEntity(entity);
			CloseableHttpResponse response = client.execute(post);

			//get token and operationURL using response
			JsonFactory f = new JsonFactory();
			JsonParser jp = f.createJsonParser(response.getEntity().getContent());

			JsonToken token;
			boolean tokenTag = false;
			boolean idTag = false;
			boolean nameTag = false;
			boolean isCloudFiles = false;
			boolean publicUrlTag = false;

			while((token = jp.nextToken()) != null){
				if(token == JsonToken.FIELD_NAME){
					if(jp.getCurrentName().equals("token"))
						tokenTag = true;
					else if(tokenTag && jp.getCurrentName().equals("id"))
						idTag = true;
					else if(jp.getCurrentName().equals("name"))
						nameTag = true;
					else if(isCloudFiles && jp.getCurrentName().equals("publicURL"))
						publicUrlTag = true;
				}
				if(token == JsonToken.VALUE_STRING){
					if(tokenTag && idTag){
						tokenId = jp.getText();
						tokenTag = idTag = false;
					}else if(nameTag && jp.getText().equals("cloudFiles")){
						isCloudFiles = true;
					}else if(publicUrlTag){
						operationURL = jp.getText();
						publicUrlTag = isCloudFiles = nameTag = false;
					}

				}
			}
			response.close();

			//list buckets
			HttpGet get = new HttpGet(operationURL+"/");
			get.addHeader("X-Auth-Token", tokenId);
			get.addHeader("Accept", "application/json");

			response = client.execute(get);
			entity = response.getEntity();

			LinkedList<String> l = new LinkedList<String>();
			if(entity == null || entity.getContent() == null){
				response.close();
			}
			jp = f.createJsonParser(entity.getContent());
			boolean next=false;
			JsonToken to;
			while((to = jp.nextToken()) != null){
				if(to == JsonToken.FIELD_NAME){
					if(jp.getCurrentName().equals("name"))
						next=true;
				}
				if(to == JsonToken.VALUE_STRING){
					if(next){
						l.add(jp.getText());
						next=false;
					}
				}
			}
			response.close();
			LinkedList<String> p = new LinkedList<>();
			//list objects per bucket
			for(String cont : l){
				if(cont.startsWith("charon-"+clientId)){
					System.out.println("RACKSPACE: Deleting bucket " + cont);
					get = new HttpGet(operationURL+"/"+cont);
					get.addHeader("X-Auth-Token", tokenId);
					get.addHeader("Accept", "application/json");

					response = client.execute(get);
					entity = response.getEntity();

					if(entity == null || entity.getContent() == null){
						response.close();
					}
					jp = f.createJsonParser(entity.getContent());
					next=false;
					to = null;
					while((to = jp.nextToken()) != null){
						if(to == JsonToken.FIELD_NAME){
							if(jp.getCurrentName().equals("name"))
								next=true;
						}
						if(to == JsonToken.VALUE_STRING){
							if(next){
								p.add(jp.getText());
								next=false;
							}
						}
					}
					response.close();

					//delete objects
					for(String obj : p){
						HttpDelete delete = new HttpDelete(operationURL+"/"+cont+"/"+obj);
						delete.addHeader("X-Auth-Token", tokenId);
						delete.addHeader("Accept", "application/json");
						response = client.execute(delete);
						response.close();
						System.out.println(".");
					}

					HttpDelete delete = new HttpDelete(operationURL+"/"+cont+"/");
					delete.addHeader("X-Auth-Token", tokenId);
					delete.addHeader("Accept", "application/json");
					response = client.execute(delete);
					response.close();

				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static String[] readCredentials(String cloudRef) throws FileNotFoundException, ParseException{
		Scanner sc=new Scanner(new File(charonCoCFileName));
		String[] creds = new String[2];
		String line = "";
		String [] splitLine;
		boolean foundCloud = false;
		while(sc.hasNext()){
			line = sc.nextLine();
			if(line.startsWith("#") || line.equals(""))
				continue;
			splitLine = line.split("=", 2);
			if(splitLine[1].equals(cloudRef)){
				foundCloud = true;
			}else if(foundCloud){
				if(splitLine[0].equals("accessKey")){
					creds[0] = splitLine[1];
				}
				if(splitLine[0].equals("secretKey")){
					creds[1] = splitLine[1];
					foundCloud = false;
				}
			}
		}
		return creds;
	}
}
