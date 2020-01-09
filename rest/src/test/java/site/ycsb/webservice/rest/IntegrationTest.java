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

package site.ycsb.webservice.rest;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import javax.servlet.ServletException;

import org.apache.catalina.Context;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.Tomcat;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.Assertion;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;
import org.junit.runners.MethodSorters;

import site.ycsb.Client;
import site.ycsb.DBException;
import site.ycsb.webservice.rest.Utils;

/**
 * Integration test cases to verify the end to end working of the rest-binding
 * module. It performs these steps in order. 1. Runs an embedded Tomcat
 * server with a mock RESTFul web service. 2. Invokes the {@link Client} 
 * class with the required parameters to start benchmarking the mock REST
 * service. 3. Compares the response stored in the output file by {@link Client}
 * class with the response expected. 4. Stops the embedded Tomcat server.
 * Cases for verifying the handling of different HTTP status like 2xx & 5xx have
 * been included in success and failure test cases.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class IntegrationTest {

  @Rule
  public final ExpectedSystemExit exit = ExpectedSystemExit.none();

  private static int port = 8080;
  private static Tomcat tomcat;
  private static final String WORKLOAD_FILEPATH =  IntegrationTest.class.getClassLoader().getResource("workload_rest").getPath();
  private static final String TRACE_FILEPATH = IntegrationTest.class.getClassLoader().getResource("trace.txt").getPath();
  private static final String ERROR_TRACE_FILEPATH = IntegrationTest.class.getClassLoader().getResource("error_trace.txt").getPath();
  private static final String RESULTS_FILEPATH = IntegrationTest.class.getClassLoader().getResource(".").getPath() + "results.txt";

  @BeforeClass
  public static void init() throws ServletException, LifecycleException, FileNotFoundException, IOException,
      DBException, InterruptedException {
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
  }

  @AfterClass
  public static void cleanUp() throws LifecycleException {
    tomcat.stop();
  }

  // All read operations during benchmark are executed successfully with an HTTP OK status.
  @Test
  public void testReadOpsBenchmarkSuccess() throws InterruptedException {
    exit.expectSystemExit();
    exit.checkAssertionAfterwards(new Assertion() {
      @Override
      public void checkAssertion() throws Exception {
        List<String> results = Utils.read(RESULTS_FILEPATH);
        assertEquals(true, results.contains("[READ], Return=OK, 1"));
        Utils.delete(RESULTS_FILEPATH);
      }
    });
    Client.main(getArgs(TRACE_FILEPATH, 1, 0, 0, 0));
  }
  
  //All read operations during benchmark are executed with an HTTP 500 error.
  @Test
  public void testReadOpsBenchmarkFailure() throws InterruptedException {
    exit.expectSystemExit();
    exit.checkAssertionAfterwards(new Assertion() {
      @Override
      public void checkAssertion() throws Exception {
        List<String> results = Utils.read(RESULTS_FILEPATH);
        assertEquals(true, results.contains("[READ], Return=ERROR, 1"));
        Utils.delete(RESULTS_FILEPATH);
      }
    });
    Client.main(getArgs(ERROR_TRACE_FILEPATH, 1, 0, 0, 0));
  }
  
  //All insert operations during benchmark are executed successfully with an HTTP OK status.
  @Test
  public void testInsertOpsBenchmarkSuccess() throws InterruptedException {
    exit.expectSystemExit();
    exit.checkAssertionAfterwards(new Assertion() {
      @Override
      public void checkAssertion() throws Exception {
        List<String> results = Utils.read(RESULTS_FILEPATH);
        assertEquals(true, results.contains("[INSERT], Return=OK, 1"));
        Utils.delete(RESULTS_FILEPATH);
      }
    });
    Client.main(getArgs(TRACE_FILEPATH, 0, 1, 0, 0));
  }
  
  //All read operations during benchmark are executed with an HTTP 500 error.
  @Test
  public void testInsertOpsBenchmarkFailure() throws InterruptedException {
    exit.expectSystemExit();
    exit.checkAssertionAfterwards(new Assertion() {
      @Override
      public void checkAssertion() throws Exception {
        List<String> results = Utils.read(RESULTS_FILEPATH);
        assertEquals(true, results.contains("[INSERT], Return=ERROR, 1"));
        Utils.delete(RESULTS_FILEPATH);
      }
    });
    Client.main(getArgs(ERROR_TRACE_FILEPATH, 0, 1, 0, 0));
  }

  //All update operations during benchmark are executed successfully with an HTTP OK status.
  @Test
  public void testUpdateOpsBenchmarkSuccess() throws InterruptedException {
    exit.expectSystemExit();
    exit.checkAssertionAfterwards(new Assertion() {
      @Override
      public void checkAssertion() throws Exception {
        List<String> results = Utils.read(RESULTS_FILEPATH);
        assertEquals(true, results.contains("[UPDATE], Return=OK, 1"));
        Utils.delete(RESULTS_FILEPATH);
      }
    });
    Client.main(getArgs(TRACE_FILEPATH, 0, 0, 1, 0));
  }
  
  //All read operations during benchmark are executed with an HTTP 500 error.
  @Test
  public void testUpdateOpsBenchmarkFailure() throws InterruptedException {
    exit.expectSystemExit();
    exit.checkAssertionAfterwards(new Assertion() {
      @Override
      public void checkAssertion() throws Exception {
        List<String> results = Utils.read(RESULTS_FILEPATH);
        assertEquals(true, results.contains("[UPDATE], Return=ERROR, 1"));
        Utils.delete(RESULTS_FILEPATH);
      }
    });
    Client.main(getArgs(ERROR_TRACE_FILEPATH, 0, 0, 1, 0));
  }

  //All delete operations during benchmark are executed successfully with an HTTP OK status.
  @Test
  public void testDeleteOpsBenchmarkSuccess() throws InterruptedException {
    exit.expectSystemExit();
    exit.checkAssertionAfterwards(new Assertion() {
      @Override
      public void checkAssertion() throws Exception {
        List<String> results = Utils.read(RESULTS_FILEPATH);
        assertEquals(true, results.contains("[DELETE], Return=OK, 1"));
        Utils.delete(RESULTS_FILEPATH);
      }
    });
    Client.main(getArgs(TRACE_FILEPATH, 0, 0, 0, 1));
  }
  
  //All read operations during benchmark are executed with an HTTP 500 error.
  @Test
  public void testDeleteOpsBenchmarkFailure() throws InterruptedException {
    exit.expectSystemExit();
    exit.checkAssertionAfterwards(new Assertion() {
      @Override
      public void checkAssertion() throws Exception {
        List<String> results = Utils.read(RESULTS_FILEPATH);
        assertEquals(true, results.contains("[DELETE], Return=ERROR, 1"));
        Utils.delete(RESULTS_FILEPATH);
      }
    });
    Client.main(getArgs(ERROR_TRACE_FILEPATH, 0, 0, 0, 1));
  }

  private String[] getArgs(String traceFilePath, float rp, float ip, float up, float dp) {
    String[] args = new String[25];
    args[0] = "-target";
    args[1] = "1";
    args[2] = "-t";
    args[3] = "-P";
    args[4] = WORKLOAD_FILEPATH;
    args[5] = "-p";
    args[6] = "url.prefix=http://127.0.0.1:"+port+"/webService/rest/resource/";
    args[7] = "-p";
    args[8] = "url.trace.read=" + traceFilePath;
    args[9] = "-p";
    args[10] = "url.trace.insert=" + traceFilePath;
    args[11] = "-p";
    args[12] = "url.trace.update=" + traceFilePath;
    args[13] = "-p";
    args[14] = "url.trace.delete=" + traceFilePath;
    args[15] = "-p";
    args[16] = "exportfile=" + RESULTS_FILEPATH;
    args[17] = "-p";
    args[18] = "readproportion=" + rp;
    args[19] = "-p";
    args[20] = "updateproportion=" + up;
    args[21] = "-p";
    args[22] = "deleteproportion=" + dp;
    args[23] = "-p";
    args[24] = "insertproportion=" + ip;
    return args;
  }

  private static ServletContainer resourceConfig() {
    return new ServletContainer(new ResourceConfig(new ResourceLoader().getClasses()));
  }

}