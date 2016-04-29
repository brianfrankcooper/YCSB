package com.yahoo.ycsb.webservice.rest;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.Assert.assertEquals;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockClassRule;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

/**
 * Mocking REST services with the easiest and the most straight forward usage
 * using stubbing. Performs these steps in order. 1. Mocks the web server part
 * of the rest services. 2. Invokes the rest client to perform HTTP operations
 * on the server. 3. Compares the response received with the expected response.
 * Various cases like - success, failure, timeout, non-implemented etc have been
 * demonstrated.
 * 
 * @author shivam.maharshi
 */
public class RestClientTest {

	@ClassRule
	public static WireMockClassRule wireMockRule = new WireMockClassRule(
			WireMockConfiguration.wireMockConfig().port(8080));

	@Rule
	public WireMockClassRule instanceRule = wireMockRule;

	private static RestClient rc = new RestClient();
	private static final String RESPONSE_TAG = "response";
	private static final String DATA_TAG = "data";
	private static final String SUCCESS_RESPONSE = "<response>success</response>";
	private static final String NOT_FOUND_RESPONSE = "<response>not found</response>";
	private static final String INPUT_DATA = "<field1>one</field1><field2>two</field2>";
	private static final int RESPONSE_DELAY = 2000; // 2 seconds.
	private static final String resource = "1";

	@BeforeClass
	public static void init() throws FileNotFoundException, IOException, DBException {
		Properties props = new Properties();
		props.load(new FileReader("src/test/resources/workload_rest"));
		rc.setProperties(props);
		rc.init();
		wireMockRule.start();
	}

	@AfterClass
	public static void cleanUp() throws DBException {
		rc.cleanup();
		wireMockRule.shutdownServer();
	}

	@Test
	// Read success.
	public void read_200() {
		stubFor(get(urlEqualTo("/myService/1")).withHeader("Accept", equalTo("*/*")).willReturn(
				aResponse().withStatus(200).withHeader("Content-Type", "text/xml").withBody(SUCCESS_RESPONSE)));
		HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
		Status status = rc.read(null, resource, null, result);
		assertEquals(Status.OK, status);
		assertEquals(result.get(RESPONSE_TAG).toString(), SUCCESS_RESPONSE);
	}

	@Test
	// Not found error.
	public void read_400() {
		stubFor(get(urlEqualTo("/myService/1")).withHeader("Accept", equalTo("*/*")).willReturn(
				aResponse().withStatus(404).withHeader("Content-Type", "text/xml").withBody(NOT_FOUND_RESPONSE)));
		HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
		Status status = rc.read(null, resource, null, result);
		assertEquals(Status.NOT_FOUND, status);
		assertEquals(result.get(RESPONSE_TAG).toString(), NOT_FOUND_RESPONSE);
	}
	
	@Test
	public void read_403() {
		stubFor(get(urlEqualTo("/myService/1")).withHeader("Accept", equalTo("*/*")).willReturn(
				aResponse().withStatus(403).withHeader("Content-Type", "text/xml")));
		HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
		Status status = rc.read(null, resource, null, result);
		assertEquals(Status.FORBIDDEN, status);
	}

	@Test
	// Insert success.
	public void insert_200() {
		stubFor(post(urlEqualTo("/myService/1")).withHeader("Accept", equalTo("*/*"))
				.withRequestBody(equalTo(INPUT_DATA)).willReturn(
						aResponse().withStatus(200).withHeader("Content-Type", "text/xml").withBody(SUCCESS_RESPONSE)));
		HashMap<String, ByteIterator> data = new HashMap<String, ByteIterator>();
		data.put(DATA_TAG, new StringByteIterator(INPUT_DATA));
		Status status = rc.insert(null, resource, data);
		assertEquals(Status.OK, status);
	}

	@Test
	// Response delay will cause execution timeout exception.
	public void insert_500() {
		stubFor(post(urlEqualTo("/myService/1")).withHeader("Accept", equalTo("*/*"))
				.withRequestBody(equalTo(INPUT_DATA)).willReturn(aResponse().withStatus(500)
						.withHeader("Content-Type", "text/xml").withFixedDelay(RESPONSE_DELAY)));
		HashMap<String, ByteIterator> data = new HashMap<String, ByteIterator>();
		data.put(DATA_TAG, new StringByteIterator(INPUT_DATA));
		Status status = rc.insert(null, resource, data);
		assertEquals(Status.ERROR, status);
	}

	@Test
	// Delete success.
	public void delete_200() {
		stubFor(delete(urlEqualTo("/myService/1")).withHeader("Accept", equalTo("*/*")).willReturn(
				aResponse().withStatus(200).withHeader("Content-Type", "text/xml")));
		Status status = rc.delete(null, resource);
		assertEquals(Status.OK, status);
	}
	
	@Test
	public void delete_500() {
		stubFor(delete(urlEqualTo("/myService/1")).withHeader("Accept", equalTo("*/*")).willReturn(
				aResponse().withStatus(500).withHeader("Content-Type", "text/xml")));
		Status status = rc.delete(null, resource);
		assertEquals(Status.ERROR, status);
	}

	@Test
	public void update() {
		assertEquals(Status.NOT_IMPLEMENTED, rc.update(null, resource, null));
	}
	
	@Test
	public void scan() {
		assertEquals(Status.NOT_IMPLEMENTED, rc.scan(null, null, 0, null, null));
	}

}
