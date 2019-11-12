package com.yahoo.ycsb.db;

import org.jolokia.client.J4pClient;
import org.jolokia.client.exception.J4pException;
import org.jolokia.client.request.J4pReadRequest;
import org.jolokia.client.request.J4pRequest;
import org.jolokia.client.request.J4pResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MalformedObjectNameException;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.util.*;

/**
 * Collecting Cassandra performance.
 */
public final class PerformanceStateCollector implements Runnable {
  private static String port = "8778";
  private static String defaultIP = "127.0.0.1";
  private static String[] rates = {"OneMinuteRate"};
  private static String resultsDir = "/home/csd/cassandra-strict-slo/performance";

  private String threshold;
  private String load;

  private double readThroughputAvg = 0.0;
  private double writeThroughputAvg = 0.0;
  private double[] readThroughput;
  private double[] writeThroughput;
  private String[] nodes;
  private List<J4pClient> clients;
  private List<J4pRequest> metricRequests;

  private boolean isRunning = false;
  private Thread t;

  /**
   * Create a performance state collector that collects state from it IPs specified.
   *
   * @param nodes IP addresses of the nodes
   */
  PerformanceStateCollector(String[] nodes, String threshold, String load) {
    // Setting up averages
    readThroughput = new double[nodes.length];
    writeThroughput = new double[nodes.length];

    this.threshold = threshold;
    this.load = load;

    // Setting up the clients for the different nodes
    this.nodes = nodes;
    this.clients = new LinkedList<>();
    for (String node : nodes) {
      String basePath = String.format("http://%s:%s/jolokia/", node, port);
      J4pClient j4pClient = new J4pClient(basePath);
      this.clients.add(j4pClient);
    }

    // Creating the list of metrics
    try {
      this.metricRequests = new LinkedList<>();
      this.metricRequests.add(new J4pReadRequest(
          "org.apache.cassandra.metrics:type=ClientRequest,scope=Read,name=Latency"));
      this.metricRequests.add(new J4pReadRequest(
          "org.apache.cassandra.metrics:type=ClientRequest,scope=Write,name=Latency"));

    } catch (MalformedObjectNameException e) {
      e.printStackTrace();
    }
  }

  /**
   * Collect all metrics once.
   */
  public void collectCurrentMetrics() {
    try {
      int currentId = 0;
      double tempReadSum = 0;
      double tempWriteSum = 0;

      for (J4pClient client : clients) {
        List<J4pResponse<J4pRequest>> responses = client.execute(metricRequests);
        Iterator<J4pResponse<J4pRequest>> iterator = responses.iterator();

        Map value = iterator.next().getValue();
        String rrate = (String) value.get("OneMinuteRate");
        value = iterator.next().getValue();
        String wrate = (String) value.get("OneMinuteRate");

        // Parsing the values and adding
        readThroughput[currentId] = Integer.parseInt(rrate);
        writeThroughput[currentId] = Integer.parseInt(wrate);
        tempReadSum += readThroughput[currentId];
        tempWriteSum += writeThroughput[currentId];

        currentId++;
      }

      readThroughputAvg = tempReadSum / this.clients.size();
      writeThroughputAvg = tempWriteSum / this.clients.size();
    } catch (J4pException e) {
      e.printStackTrace();
    }
  }

  /**
   * Collect benchmark metrics and save them to file.
   *
   * @throws Exception
   */
  private void performBenchmarkDataCollection(String thresholdString, String loadString) throws Exception {

    // Setting up request objects
    List<J4pRequest> requestList = new LinkedList<>();
    List<String[]> valueList = new LinkedList<>();
    requestList.add(new J4pReadRequest("java.lang:type=Memory", "HeapMemoryUsage"));
    valueList.add(new String[]{"used"});
    requestList.add(new J4pReadRequest("org.apache.cassandra.metrics:type=ClientRequest,scope=Read,name=Latency"));
    valueList.add(rates);
    requestList.add(new J4pReadRequest("org.apache.cassandra.metrics:type=ClientRequest,scope=Write,name=Latency"));
    valueList.add(rates);
    requestList.add(new J4pReadRequest("org.apache.cassandra.metrics:type=CommitLog,name=PendingTasks"));
    valueList.add(new String[]{"Value"});
    requestList.add(new J4pReadRequest("org.apache.cassandra.metrics:type=CommitLog,name=WaitingOnCommit"));
    valueList.add(rates);

    // Ensure directory exists
    File directory = new File(resultsDir);
    if (!directory.exists()) {
      directory.mkdirs();
    }

    // Create file writers
    List<PrintWriter> writers = new LinkedList<>();
    for (String ip : this.nodes) {
      String filename = String.format("%s/state_%s_%s_%s", resultsDir, ip, thresholdString, loadString);
      System.out.println("Creating peformance file: " + filename);
      PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(filename)));
      pw.println("Timestamp, MemoryUsed, ReadLatency1, WriteLatency1, PendingTasks, WaitingOnCommit1, ");

      writers.add(pw);
    }

    while (isRunning) {
      Thread.sleep(1000);

      Iterator<PrintWriter> writerIT = writers.iterator();
      for (J4pClient client : clients) {
        List<J4pResponse<J4pRequest>> responses = client.execute(requestList);
        Iterator<String[]> valueIt = valueList.iterator();

        PrintWriter writer = writerIT.next();
        writer.printf("%s,", new Timestamp(new Date().getTime()).toString());

        for (J4pResponse<J4pRequest> response : responses) {
          String[] values = valueIt.next();
          Map responseMap = response.getValue();
          for (String value : values) {
            writer.printf("%s, ", responseMap.get(value));
          }
        }

        writer.println("");
      }
    }

    for (PrintWriter writer : writers) {
      writer.close();
    }
  }

  @Override
  public void run() {
    try {
      System.out.println("Performance thread started: " + new Timestamp(new Date().getTime()).toString());
      this.performBenchmarkDataCollection(this.threshold, this.load);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void startThread() {
    t = new Thread(this);
    this.isRunning = true;
    t.start();
  }

  public void stopThread() {
    if (this.isRunning) {
      System.out.println("Performance collection stopped: " + new Timestamp(new Date().getTime()).toString());
      this.isRunning = false;
      try {
        t.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public static void main(String[] args) throws Exception {
//    performBenchmarkDataCollection("10.0.0.11", "");
  }
}
