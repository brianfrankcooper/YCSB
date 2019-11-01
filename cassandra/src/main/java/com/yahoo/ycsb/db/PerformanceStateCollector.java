package com.yahoo.ycsb.db;

import org.jolokia.client.J4pClient;
import org.jolokia.client.request.J4pReadRequest;
import org.jolokia.client.request.J4pRequest;
import org.jolokia.client.request.J4pResponse;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Collecting Cassandra performance.
 */
public final class PerformanceStateCollector {
  private static String port = "8778";
  private static String url = "http://127.0.0.1";
  private PerformanceStateCollector() {
    // NOP
  }

  //CHECKSTYLE:OFF
  public static void main(String[] args) throws Exception {
    String basePath = String.format("%s:%s/jolokia/", url, port);
    J4pClient j4pClient = new J4pClient(basePath);

    List<J4pRequest> requestList = new LinkedList<>();
    requestList.add(new J4pReadRequest("java.lang:type=Memory", "HeapMemoryUsage"));
    requestList.add(new J4pReadRequest("org.apache.cassandra.metrics:type=ClientRequest,scope=Read,name=Latency"));
    requestList.add(new J4pReadRequest("org.apache.cassandra.metrics:type=ClientRequest,scope=Read,name=Timeouts"));
    requestList.add(new J4pReadRequest("org.apache.cassandra.metrics:type=ClientRequest,scope=Write,name=Latency"));
    requestList.add(new J4pReadRequest("org.apache.cassandra.metrics:type=ClientRequest,scope=Write,name=Timeouts"));
    requestList.add(new J4pReadRequest("org.apache.cassandra.metrics:type=CommitLog,name=PendingTasks"));
    requestList.add(new J4pReadRequest("org.apache.cassandra.metrics:type=CommitLog,name=WaitingOnCommit"));
    requestList.add(
        new J4pReadRequest("org.apache.cassandra.metrics:keyspace=ycsb,name=ReadLatency,scope=usertable,type=Table")
    );
    requestList.add(
        new J4pReadRequest("org.apache.cassandra.metrics:keyspace=ycsb,name=WriteLatency,scope=usertable,type=Table")
    );

    List<J4pResponse<J4pRequest>> responses = j4pClient.execute(requestList);

    // Memory related
    System.out.println("Memory related:");
    Iterator<J4pResponse<J4pRequest>> iterator = responses.iterator();
    Map memVals = iterator.next().getValue();
    System.out.println("Memory used: " + memVals.get("used") + " b");

    // Latency
    System.out.println("\nRead");
    Map readLatencyVals = iterator.next().getValue();
    // We also have the list "RecentValues" which contains all the recent values here
    System.out.println("Latency: One minute rate: " + readLatencyVals.get("OneMinuteRate") + " us");
    System.out.println("Latency: Five minute rate: " + readLatencyVals.get("FiveMinuteRate") + " us");

    readLatencyVals = iterator.next().getValue();
    System.out.println("Timeouts: One minute rate: " + readLatencyVals.get("OneMinuteRate") + " e/s");
    System.out.println("Timeouts: Five minute rate: " + readLatencyVals.get("FiveMinuteRate") + " e/s");

    System.out.println("\nWrite");
    Map writeLatencyVals = iterator.next().getValue();
    System.out.println("Latency: One minute rate: " + writeLatencyVals.get("OneMinuteRate") + " us");
    System.out.println("Latency: Five minute rate: " + writeLatencyVals.get("FiveMinuteRate") + " us");

    writeLatencyVals = iterator.next().getValue();
    System.out.println("Timeouts: One minute rate: " + writeLatencyVals.get("OneMinuteRate") + " e/s");
    System.out.println("Timeouts: Five minute rate: " + writeLatencyVals.get("FiveMinuteRate") + " e/s");

    System.out.println("Commit log");
    Map commitLogVals = iterator.next().getValue();
    System.out.println("Pending tasks: " + commitLogVals.get("Value"));
    commitLogVals = iterator.next().getValue();
    System.out.println("Waiting on commit: " + commitLogVals.get("OneMinuteRate"));

    System.out.println("Table info");
    Map tableVals = iterator.next().getValue();
    System.out.println("Read: One minute rate: " + tableVals.get("OneMinuteRate") + " e/s");
    System.out.println("Read: Five minute rate: " + tableVals.get("FiveMinuteRate") + " e/s");
    tableVals = iterator.next().getValue();
    System.out.println("One minute rate: " + tableVals.get("OneMinuteRate") + " e/s");
    System.out.println("Five minute rate: " + tableVals.get("FiveMinuteRate") + " e/s");

  }
  //CHECKSTYLE:ON
}
