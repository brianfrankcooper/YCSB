/**
 * Copyright (c) 2016 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.webservice.rest;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import java.util.zip.GZIPOutputStream;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockClassRule;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.webservice.rest.Utils;

/**
 * Mocking REST services with the easiest and the most straight forward usage
 * using stubbing. Performs these steps in order. 1. Mocks the web server part
 * of the rest services. 2. Invokes the rest client to perform HTTP operations
 * on the server. 3. Compares the response received with the expected response.
 * Various cases like - success, failure, timeout, non-implemented etc have been
 * demonstrated.
 */
public class RestClientTest {

  @ClassRule
  public static WireMockClassRule wireMockRule;

  @Rule
  public WireMockClassRule instanceRule = wireMockRule;

  private static int port = 8080;
  private static RestClient rc = new RestClient();
  private static final String RESPONSE_TAG = "response";
  private static final String DATA_TAG = "data";
  private static final String SUCCESS_RESPONSE = "<response>success</response>";
  private static byte[] GZIP_SUCCESS_REPONSE = null;
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
    ByteArrayOutputStream out = new ByteArrayOutputStream(SUCCESS_RESPONSE.length());
    GZIPOutputStream gzip = new GZIPOutputStream(out);
    gzip.write(SUCCESS_RESPONSE.getBytes("UTF-8"));
    gzip.close();
    GZIP_SUCCESS_REPONSE = out.toByteArray();
    // Get unique port.
    while (!Utils.available(port)) {
      port++;
    }
    wireMockRule = new WireMockClassRule(WireMockConfiguration.wireMockConfig().port(port));
    wireMockRule.start();
  }

  @AfterClass
  public static void cleanUp() throws DBException {
    rc.cleanup();
    wireMockRule.shutdownServer();
  }

  // Read success.
  @Test
  public void read_200() {
    stubFor(get(urlEqualTo("/myService/1")).withHeader("Accept", equalTo("*/*"))
        .willReturn(aResponse().withStatus(200).withHeader("Content-Type", "text/xml").withBody(SUCCESS_RESPONSE)));
    HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
    Status status = rc.read(null, resource, null, result);
    assertEquals(Status.OK, status);
    assertEquals(result.get(RESPONSE_TAG).toString(), SUCCESS_RESPONSE);
  }

  // Read GZIP response success. Not supported currently.
  @Ignore
  @Test
  public void readZip_200() throws DBException {
    stubFor(get(urlEqualTo("/myService/1")).withHeader("Accept", equalTo("*/*")).willReturn(aResponse().withStatus(200)
        .withHeader("Content-Type", "text/xml").withHeader("Content-Encoding", "gzip").withBody(GZIP_SUCCESS_REPONSE)));
    HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
    Status status = rc.read(null, resource, null, result);
    assertEquals(Status.OK, status);
    assertEquals(result.get(RESPONSE_TAG).toString(), SUCCESS_RESPONSE);
  }

  // Not found error.
  @Test
  public void read_400() {
    stubFor(get(urlEqualTo("/myService/1")).withHeader("Accept", equalTo("*/*"))
        .willReturn(aResponse().withStatus(404).withHeader("Content-Type", "text/xml").withBody(NOT_FOUND_RESPONSE)));
    HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
    Status status = rc.read(null, resource, null, result);
    assertEquals(Status.NOT_FOUND, status);
    assertEquals(result.get(RESPONSE_TAG).toString(), NOT_FOUND_RESPONSE);
  }

  @Test
  public void read_403() {
    stubFor(get(urlEqualTo("/myService/1")).withHeader("Accept", equalTo("*/*"))
        .willReturn(aResponse().withStatus(403).withHeader("Content-Type", "text/xml")));
    HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
    Status status = rc.read(null, resource, null, result);
    assertEquals(Status.FORBIDDEN, status);
  }

  // Insert success.
  @Test
  public void insert_200() {
    stubFor(post(urlEqualTo("/myService/1")).withHeader("Accept", equalTo("*/*")).withRequestBody(equalTo(INPUT_DATA))
        .willReturn(aResponse().withStatus(200).withHeader("Content-Type", "text/xml").withBody(SUCCESS_RESPONSE)));
    HashMap<String, ByteIterator> data = new HashMap<String, ByteIterator>();
    data.put(DATA_TAG, new StringByteIterator(INPUT_DATA));
    Status status = rc.insert(null, resource, data);
    assertEquals(Status.OK, status);
  }

  // Response delay will cause execution timeout exception.
  @Test
  public void insert_500() {
    stubFor(post(urlEqualTo("/myService/1")).withHeader("Accept", equalTo("*/*")).withRequestBody(equalTo(INPUT_DATA))
        .willReturn(aResponse().withStatus(500).withHeader("Content-Type", "text/xml").withFixedDelay(RESPONSE_DELAY)));
    HashMap<String, ByteIterator> data = new HashMap<String, ByteIterator>();
    data.put(DATA_TAG, new StringByteIterator(INPUT_DATA));
    Status status = rc.insert(null, resource, data);
    assertEquals(Status.ERROR, status);
  }

  // Delete success.
  @Test
  public void delete_200() {
    stubFor(delete(urlEqualTo("/myService/1")).withHeader("Accept", equalTo("*/*"))
        .willReturn(aResponse().withStatus(200).withHeader("Content-Type", "text/xml")));
    Status status = rc.delete(null, resource);
    assertEquals(Status.OK, status);
  }

  @Test
  public void delete_500() {
    stubFor(delete(urlEqualTo("/myService/1")).withHeader("Accept", equalTo("*/*"))
        .willReturn(aResponse().withStatus(500).withHeader("Content-Type", "text/xml")));
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
