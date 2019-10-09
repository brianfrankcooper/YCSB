package com.yahoo.ycsb.db;

import org.jolokia.client.J4pClient;
import org.jolokia.client.request.*;

import java.util.List;
import java.util.Map;

/**
 * Collecting Cassandra performance
 */
public class PerformanceStateCollector {
//  Post to https://jsonplaceholder.typicode.com/posts
  // Get from /users

  private static String port = "8778";
  private static String url = "http://localhost";

  public void main(String[] args) throws Exception {
    String basePath = String.format("%s:%s/jolokia", url, port);
    J4pClient j4pClient = new J4pClient(basePath);

    J4pRequest req1 = new J4pExecRequest("java.lang:type=Memory", "gc");
    J4pRequest req2 = new J4pReadRequest("java.lang:type=Memory", "HeapMemoryUsage");

    J4pReadRequest req = new J4pReadRequest("java.lang:type=Memory", "HeapMemoryUsage");
    J4pReadResponse resp = j4pClient.execute(req);
    System.out.println((String) resp.getValue());

    System.out.println("------------------------------");

    List<J4pResponse<J4pRequest>> responses = j4pClient.execute(req1, req2);
    Map memVals = responses.get(1).getValue();
    System.out.println("Memory used: " + memVals.get("used"));
  }
}
