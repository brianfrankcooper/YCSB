package com.yahoo.ycsb.db;

import org.jolokia.client.J4pClient;
import org.jolokia.client.exception.J4pException;
import org.jolokia.client.request.J4pReadRequest;
import org.jolokia.client.request.J4pRequest;
import org.jolokia.client.request.J4pResponse;

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
  private static String[] rates = {"OneMinuteRate", "Count"};
  private static String resultsDir = "/home/csd/cassandra-strict-slo/performance";

  private String threshold;
  private String load;
  private String prefix;
  private DynamicSpeculativeExecutionPolicy policy;

  private double readThroughputAvg = 0.0;
  private double writeThroughputAvg = 0.0;
  private double readCountAvg = 0.0;
  private double writeCountAvg = 0.0;
  private double[] readThroughput;
  private double[] writeThroughput;
  private int[] readCount;
  private int[] writeCount;
  private int[] readCountLast;
  private int[] writeCountLast;
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
  PerformanceStateCollector(String[] nodes, String threshold, String load, String prefix, DynamicSpeculativeExecutionPolicy policy) {
    // Setting up averages
    readThroughput = new double[nodes.length];
    writeThroughput = new double[nodes.length];

    this.threshold = threshold;
    this.load = load;
    this.prefix = prefix;
    this.policy = policy;

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
      int tempReadCountSum = 0;
      int tempWriteCountSum = 0;

      for (J4pClient client : clients) {
        List<J4pResponse<J4pRequest>> responses = client.execute(metricRequests);
        Iterator<J4pResponse<J4pRequest>> iterator = responses.iterator();

        Map value = iterator.next().getValue();
        String rrate = (String) value.get("OneMinuteRate");
        String rcount = (String) value.get("Count");
        value = iterator.next().getValue();
        String wrate = (String) value.get("OneMinuteRate");
        String wcount = (String) value.get("Count");

        // Parsing the values and adding
        readThroughput[currentId] = Integer.parseInt(rrate);
        writeThroughput[currentId] = Integer.parseInt(wrate);
        tempReadSum += readThroughput[currentId];
        tempWriteSum += writeThroughput[currentId];

        int rNewCount = Integer.parseInt(rcount);
        int wNewCount = Integer.parseInt(wcount);
        readCount[currentId] = rNewCount - readCountLast[currentId];
        writeCount[currentId] = wNewCount - writeCountLast[currentId];
        readCountLast[currentId] = rNewCount;
        writeCountLast[currentId] = wNewCount;
        tempReadCountSum += readCount[currentId];
        tempWriteCountSum += writeCount[currentId];

        currentId++;
      }

      readThroughputAvg = tempReadSum / this.clients.size();
      writeThroughputAvg = tempWriteSum / this.clients.size();
      readCountAvg = ((double) tempReadCountSum) / this.clients.size();
      writeCountAvg = ((double) tempWriteCountSum) / this.clients.size();
    } catch (J4pException e) {
      e.printStackTrace();
    }
  }

  private int calculateDelay(double readMean, double readVariance, double writeMean, double writeVariance) {
    // Hardcoded from model
    double delay = 13.803020995708664 - 0.08933508 * readMean + 0.04679561 * readVariance + 1.66462934 * writeMean - 2.25407561 * writeVariance;
    return (int) Math.round(delay);
  }

  /**
   * Collect benchmark metrics and save them to file.
   *
   * @throws Exception
   */
  private void performBenchmarkDataCollection(String thresholdString, String loadString, String prefix) throws Exception {

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
    valueList.add(new String[]{"OneMinuteRate"});

    // Ensure directory exists
    File directory = new File(resultsDir);
    if (!directory.exists()) {
      directory.mkdirs();
    }

    // Create file writers
    List<PrintWriter> writers = new LinkedList<>();
    for (String ip : this.nodes) {
      String filename = String.format("%s/%s_%s_%s_%s", resultsDir, prefix, thresholdString, loadString, ip);
      System.out.println("Creating peformance file: " + filename);
      PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(filename)));
      pw.println("Timestamp, MemoryUsed, ReadLatency1, ReadCount, WriteLatency1, WriteCount, PendingTasks, WaitingOnCommit1, SRDelay, ");

      writers.add(pw);
    }

    // Create a list of previous counts
    int[] lastReadCounts = new int[clients.size()];
    int[] lastWriteCounts = new int[clients.size()];
    Arrays.fill(lastReadCounts, 0);
    Arrays.fill(lastWriteCounts, 0);

    while (isRunning) {
      Thread.sleep(1000);

      Iterator<PrintWriter> writerIT = writers.iterator();

      int[] readCounts = new int[clients.size()];
      int[] writeCounts = new int[clients.size()];

      int clientIndex = 0;
      for (J4pClient client : clients) {
        List<J4pResponse<J4pRequest>> responses = client.execute(requestList);
        Iterator<String[]> valueIt = valueList.iterator();

        PrintWriter writer = writerIT.next();
        writer.printf("%s,", new Timestamp(new Date().getTime()).toString());

        int valueIndex = 0;
        for (J4pResponse<J4pRequest> response : responses) {
          String[] values = valueIt.next();
          Map responseMap = response.getValue();
          String value = responseMap.get(value);
          for (String value : values) {
            writer.printf("%s, ", value);
          }

          if(valueIndex == 3) { // ReadCount
            readCounts[clientIndex] = Integer.parseInt(value);
          } else if (valueIndex == 5) { // WriteCount
            writeCounts[clientIndex] = Integer.parseInt(value);
          }

          valueIndex++;
        }

        clientIndex++;
      }

      // Calculate per second throuhgput
      int[] readThroughputs = new int[clients.size()];
      int[] writeThroughputs = new int[clients.size()];

      for(int i = 0; i < readThroughputs.length; i++) {
        readThroughputs[i] = readCounts[i] - lastReadCounts[i];
        writeThroughputs[i] = writeCounts[i] - lastWriteCounts[i];
      }

      // Save the current total count
      lastReadCounts = readCounts;
      lastWriteCounts = writeCounts;

      // Calculate mean and variance
      int readSum = 0;
      int writeSum = 0;
      for (int i = 0; i < readThroughputs.length; i++) {
        readSum += readThroughputs[i];
        writeSum += writeThroughputs[i];
      }

      double readMean = readSum / readThroughputs.length;
      double writeMean = writeSum / writeThroughputs.length;

      double squaredReadSums = 0;
      double squaredWriteSums = 0;

      for (int i = 0; i < readThroughputs.length; i++) {
        squaredReadSums += Math.pow((readThroughputs[i] - readMean), 2);
        squaredWriteSums += Math.pow((writeThroughputs[i] - writeMean), 2);
      }

      double readVariance = squaredReadSums / (readThroughputs.length - 1);
      double writeVariance = squaredWriteSums / (writeThroughputs.length - 1);

      int nextSRDelay = calculateDelay(readMean, readVariance, writeMean, writeVariance);
      policy.setDynamicDelay(nextSRDelay);

      for (PrintWriter writer : writers) {
        writer.printf("%s, ", nextSRDelay);
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
      this.performBenchmarkDataCollection(this.threshold, this.load, this.prefix);
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
