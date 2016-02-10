package com.yahoo.ycsb.webservice.rest;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

/**
 * Class responsible for making web service requests for benchmarking purpose.
 * Using Apache HttpClient over standard Java HTTP API as this is more flexible
 * and provides better functionality. For example HttpClient can automatically
 * handle Redirects and Proxy Authentication which the standard Java API don't.
 * 
 * @author shivam.maharshi
 */
public class RestClient extends DB {

	private static final String URL_PREFIX = "urlprefix";
	private static final String LOG_ENABLED = "enablelog";
	private static boolean logEnabled = false;
	private String urlPrefix;
	private Properties props;
	private CloseableHttpClient client;

	@Override
	public void init() throws DBException {
		props = getProperties();
		urlPrefix = props.getProperty(URL_PREFIX, "");
		logEnabled = Boolean.valueOf(props.getProperty(LOG_ENABLED, "false").trim());
		setupClient();
	}

	private void setupClient() {
		RequestConfig.Builder requestBuilder = RequestConfig.custom();
		requestBuilder = requestBuilder.setConnectTimeout(200);
		requestBuilder = requestBuilder.setConnectionRequestTimeout(200);
		requestBuilder = requestBuilder.setSocketTimeout(200);
		HttpClientBuilder clientBuilder = HttpClientBuilder.create().setDefaultRequestConfig(requestBuilder.build());
		this.client = clientBuilder.setUserAgent("Mozilla/5.0").build();
	}

	@Override
	public Status read(String table, String endpoint, Set<String> fields, HashMap<String, ByteIterator> result) {
		int responseCode;
		try {
			responseCode = httpGet(urlPrefix + endpoint, result);
		} catch (IOException e) {
			responseCode = handleExceptions(e);
		}
		if (logEnabled)
			System.out.println("GET Request : " + urlPrefix + endpoint + " | Response Code : " + responseCode);
		return getStatus(responseCode);
	}

	@Override
	public Status insert(String table, String endpoint, HashMap<String, ByteIterator> values) {
		int responseCode;
		try {
			responseCode = httpPost(urlPrefix + endpoint, values.get("data").toString());
		} catch (IOException e) {
			responseCode = handleExceptions(e);
		}
		if (logEnabled)
			System.out.println("POST Request : " + urlPrefix + endpoint + " | Response Code : " + responseCode);
		return getStatus(responseCode);
	}

	@Override
	public Status delete(String table, String endpoint) {
		int responseCode;
		try {
			responseCode = httpDelete(urlPrefix + endpoint);
		} catch (IOException e) {
			responseCode = handleExceptions(e);
		}
		if (logEnabled)
			System.out.println("DELETE Request : " + urlPrefix + endpoint + " | Response Code : " + responseCode);
		return getStatus(responseCode);
	}

	@Override
	public Status update(String table, String key, HashMap<String, ByteIterator> values) {
		System.out.println("Update not implemented.");
		return Status.OK;
	}
	
	@Override
	public Status scan(String table, String startkey, int recordcount, Set<String> fields,
			Vector<HashMap<String, ByteIterator>> result) {
		System.out.println("Scan operation is not supported for RESTFul Web Services client.");
		return Status.OK;
	}

	private Status getStatus(int responseCode) {
		if (responseCode / 100 == 5) {
			return Status.ERROR;
		}
		return Status.OK;
	}

	private int handleExceptions(IOException e) {
		if (e instanceof ClientProtocolException)
			return 400;
		return 500;
	}

	// Connection is automatically released back in case of an exception.
	private int httpGet(String endpoint, HashMap<String, ByteIterator> result) throws IOException {
		int responseCode = 200;
		HttpGet request = new HttpGet(endpoint);
		request.setHeader("Accept", "*/*");
		CloseableHttpResponse response = client.execute(request);
		responseCode = response.getStatusLine().getStatusCode();
		HttpEntity responseEntity = response.getEntity();
		// If null entity don't bother about connection release.
		if (responseEntity != null) {
			InputStream stream = responseEntity.getContent();
			BufferedReader reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
			StringBuffer responseContent = new StringBuffer();
			String line = "";
			while ((line = reader.readLine()) != null) {
				responseContent.append(line);
			}
			result.put("response", new StringByteIterator(responseContent.toString()));
			// Closing the input stream will trigger connection release.
			stream.close();
		}
		if (response != null)
			response.close();
		client.close();
		return responseCode;
	}

	// Connection is automatically released back in case of an exception.
	private int httpPost(String endpoint, String postData) throws IOException {
		int responseCode = 200;
		HttpPost request = new HttpPost(endpoint);
		request.setHeader("Accept", "*/*");
		request.setHeader("Accept-Language", "en-US,en;q=0.5");
		request.setHeader("Content-Type", "application/x-www-form-urlencoded");
		InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream(postData.getBytes()),
				ContentType.APPLICATION_FORM_URLENCODED);
		reqEntity.setChunked(true);
		request.setEntity(reqEntity);
		CloseableHttpResponse response = client.execute(request);
		responseCode = response.getStatusLine().getStatusCode();
		HttpEntity responseEntity = response.getEntity();
		// If null entity don't bother about connection release.
		if (responseEntity != null) {
			InputStream stream = responseEntity.getContent();
			BufferedReader reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
			StringBuffer responseContent = new StringBuffer();
			String line = "";
			while ((line = reader.readLine()) != null) {
				responseContent.append(line);
			}
			// Closing the input stream will trigger connection release.
			stream.close();
		}
		if (response != null)
			response.close();
		client.close();
		return responseCode;
	}

	// Connection is automatically released back in case of an exception.
	private int httpDelete(String endpoint) throws IOException {
		int responseCode = 200;
		HttpDelete request = new HttpDelete(endpoint);
		CloseableHttpResponse response = client.execute(request);
		responseCode = response.getStatusLine().getStatusCode();
		if (response != null)
			response.close();
		client.close();
		return responseCode;
	}

	public static void main(String[] args) throws UnsupportedEncodingException {
		String data = "Miusov, as a man man of breeding and deilcacy, could not but feel some inwrd qualms, when he reached the Father Superior's with Ivan: he felt ashamed of havin lost his temper. He felt that he ought to have disdaimed that despicable wretch, Fyodor Pavlovitch, too much to have been upset by him in Father Zossima's cell, and so to have forgotten himself. Teh monks were not to blame, in any case, he reflceted, on the steps.And if they're decent people here (and the Father Superior, I understand, is a nobleman) why not be friendly and courteous withthem? I won't argue, I'll fall in with everything, I'll win them by politness, and show them that I've nothing to do with that Aesop, thta buffoon, that Pierrot, and have merely been takken in over this affair, just as they have.";
		String postData = "section=0";
		postData += "&title=" + URLEncoder.encode("Αγορά", "UTF-8");
		postData += "&appendtext=" + data;
		postData += "&token=%2B%5C";

		RestClient rc = new RestClient();
		HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
		try {
			rc.init();
			// rc.httpPost("http://10.0.0.91/mediawiki2/api.php?action=edit&format=json", postData);
			System.out.println(System.currentTimeMillis());
			rc.httpGet("http://10.0.0.91/mediawiki2/index.php/Facebook", result);
			System.out.println(result.get("response").toArray().toString());
			System.out.println(System.currentTimeMillis());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// w.sendPost("http://52.34.20.119/mediawiki/api.php?action=edit&format=json",
		// params);
	}

}
