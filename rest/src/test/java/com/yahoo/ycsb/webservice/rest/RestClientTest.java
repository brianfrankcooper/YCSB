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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

import javax.servlet.ServletException;

import org.apache.catalina.Context;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.Tomcat;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

/**
 * Test cases to verify the {@link RestClient} of the rest-binding
 * module. It performs these steps in order. 1. Runs an embedded Tomcat
 * server with a mock RESTFul web service. 2. Invokes the {@link RestClient} 
 * class for all the various methods which make HTTP calls to the mock REST
 * service. 3. Compares the response from such calls to the mock REST
 * service with the response expected. 4. Stops the embedded Tomcat server.
 * Cases for verifying the handling of different HTTP status like 2xx, 4xx &
 * 5xx have been included in success and failure test cases.
 */
public class RestClientTest {

  private static Integer port = 8080;
  private static Tomcat tomcat;
  private static RestClient rc = new RestClient();
  private static final String RESPONSE_TAG = "response";
  private static final String DATA_TAG = "data";
  private static final String VALID_RESOURCE = "resource_valid";
  private static final String INVALID_RESOURCE = "resource_invalid";
  private static final String ABSENT_RESOURCE = "resource_absent";
  private static final String UNAUTHORIZED_RESOURCE = "resource_unauthorized";
  private static final String INPUT_DATA = "<field1>one</field1><field2>two</field2>";

  @BeforeClass
  public static void init() throws IOException, DBException, ServletException, LifecycleException, InterruptedException {
    String webappDirLocation =  IntegrationTest.class.getClassLoader().getResource("WebContent").getPath();
    while (!Utils.available(port)) {
      port++;
    }
    tomcat = new Tomcat();
    tomcat.setPort(Integer.valueOf(port));
    Context context = tomcat.addWebapp("/webService", new File(webappDirLocation).getAbsolutePath());
    Tomcat.addServlet(context, "jersey-container-servlet", resourceConfig());
    context.addServletMapping("/rest/*", "jersey-container-servlet");
    tomcat.start();
    // Allow time for proper startup.
    Thread.sleep(1000);
    Properties props = new Properties();
    props.load(new FileReader(RestClientTest.class.getClassLoader().getResource("workload_rest").getPath()));
    // Update the port value in the url.prefix property.
    props.setProperty("url.prefix", props.getProperty("url.prefix").replaceAll("PORT", port.toString()));
    rc.setProperties(props);
    rc.init();
  }

  @AfterClass
  public static void cleanUp() throws DBException {
    rc.cleanup();
  }
  
  // Read success.
  @Test
  public void read_200() {
    HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
    Status status = rc.read(null, VALID_RESOURCE, null, result);
    assertEquals(Status.OK, status);
    assertEquals(result.get(RESPONSE_TAG).toString(), "HTTP GET response to: "+ VALID_RESOURCE);
  }
  
  // Unauthorized request error.
  @Test
  public void read_403() {
    HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
    Status status = rc.read(null, UNAUTHORIZED_RESOURCE, null, result);
    assertEquals(Status.FORBIDDEN, status);
  }
  
  //Not found error.
  @Test
  public void read_404() {
    HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
    Status status = rc.read(null, ABSENT_RESOURCE, null, result);
    assertEquals(Status.NOT_FOUND, status);
  }
  
  // Server error.
  @Test
  public void read_500() {
    HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
    Status status = rc.read(null, INVALID_RESOURCE, null, result);
    assertEquals(Status.ERROR, status);
  }
  
  // Insert success.
  @Test
  public void insert_200() {
    HashMap<String, ByteIterator> data = new HashMap<String, ByteIterator>();
    data.put(DATA_TAG, new StringByteIterator(INPUT_DATA));
    Status status = rc.insert(null, VALID_RESOURCE, data);
    assertEquals(Status.OK, status);
  }
  
  @Test
  public void insert_403() {
    HashMap<String, ByteIterator> data = new HashMap<String, ByteIterator>();
    data.put(DATA_TAG, new StringByteIterator(INPUT_DATA));
    Status status = rc.insert(null, UNAUTHORIZED_RESOURCE, data);
    assertEquals(Status.FORBIDDEN, status);
  }
  
  @Test
  public void insert_404() {
    HashMap<String, ByteIterator> data = new HashMap<String, ByteIterator>();
    data.put(DATA_TAG, new StringByteIterator(INPUT_DATA));
    Status status = rc.insert(null, ABSENT_RESOURCE, data);
    assertEquals(Status.NOT_FOUND, status);
  }
  
  @Test
  public void insert_500() {
    HashMap<String, ByteIterator> data = new HashMap<String, ByteIterator>();
    data.put(DATA_TAG, new StringByteIterator(INPUT_DATA));
    Status status = rc.insert(null, INVALID_RESOURCE, data);
    assertEquals(Status.ERROR, status);
  }

  // Delete success.
  @Test
  public void delete_200() {
    Status status = rc.delete(null, VALID_RESOURCE);
    assertEquals(Status.OK, status);
  }
  
  @Test
  public void delete_403() {
    Status status = rc.delete(null, UNAUTHORIZED_RESOURCE);
    assertEquals(Status.FORBIDDEN, status);
  }
  
  @Test
  public void delete_404() {
    Status status = rc.delete(null, ABSENT_RESOURCE);
    assertEquals(Status.NOT_FOUND, status);
  }

  @Test
  public void delete_500() {
    Status status = rc.delete(null, INVALID_RESOURCE);
    assertEquals(Status.ERROR, status);
  }
  
  @Test
  public void update_200() {
    HashMap<String, ByteIterator> data = new HashMap<String, ByteIterator>();
    data.put(DATA_TAG, new StringByteIterator(INPUT_DATA));
    Status status = rc.update(null, VALID_RESOURCE, data);
    assertEquals(Status.OK, status);
  }
  
  @Test
  public void update_403() {
    HashMap<String, ByteIterator> data = new HashMap<String, ByteIterator>();
    data.put(DATA_TAG, new StringByteIterator(INPUT_DATA));
    Status status = rc.update(null, UNAUTHORIZED_RESOURCE, data);
    assertEquals(Status.FORBIDDEN, status);
  }
  
  @Test
  public void update_404() {
    HashMap<String, ByteIterator> data = new HashMap<String, ByteIterator>();
    data.put(DATA_TAG, new StringByteIterator(INPUT_DATA));
    Status status = rc.update(null, ABSENT_RESOURCE, data);
    assertEquals(Status.NOT_FOUND, status);
  }
  
  @Test
  public void update_500() {
    HashMap<String, ByteIterator> data = new HashMap<String, ByteIterator>();
    data.put(DATA_TAG, new StringByteIterator(INPUT_DATA));
    Status status = rc.update(null, INVALID_RESOURCE, data);
    assertEquals(Status.ERROR, status);
  }
  
  @Test
  public void scan() {
    assertEquals(Status.NOT_IMPLEMENTED, rc.scan(null, null, 0, null, null));
  }

  private static ServletContainer resourceConfig() {
    return new ServletContainer(new ResourceConfig(new ResourceLoader().getClasses()));
  }

}
