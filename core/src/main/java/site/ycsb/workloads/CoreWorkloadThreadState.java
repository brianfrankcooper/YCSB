package site.ycsb.workloads;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import site.ycsb.ByteIterator;
import site.ycsb.ClientThread;

/**
 * Thread state for CoreWorkload.
 */
public class CoreWorkloadThreadState {

  private int payloadOpsCount;
  private int warmupOpsCount;
  private int totalOpsCount;

  private int payloadOpsDone;
  private int warmupOpsDone;
  private int totalOpsDone;

  private List<String> batchKeysList;
  private List<Set<String>> batchFieldsList;
  private List<Map<String, ByteIterator>> batchValuesList;

  public CoreWorkloadThreadState(int payloadOpsCount, int warmupOpsCount, int totalOpsCount) {
    this.payloadOpsCount = payloadOpsCount;
    this.warmupOpsCount = warmupOpsCount;
    this.totalOpsCount = totalOpsCount;

    payloadOpsDone = 0;
    warmupOpsDone = 0;
    totalOpsDone = 0;
  }

  public CoreWorkloadThreadState(ClientThread thread) {
    this(thread.getOpsCount(), thread.getWarmupOpsCount(), thread.getTotalOpsCount());
  }

  public void initBatchArrays(int batchsize) {
    batchKeysList = new ArrayList<>(batchsize);
    batchFieldsList = new ArrayList<>(batchsize);
    batchValuesList = new ArrayList<>(batchsize);
  }

  public int getPayloadOpsCount() {
    return payloadOpsCount;
  }

  public int getWarmupOpsCount() {
    return warmupOpsCount;
  }

  public int getTotalOpsCount() {
    return totalOpsCount;
  }

  public int getPayloadOpsDone() {
    return payloadOpsDone;
  }

  public int getWarmupOpsDone() {
    return warmupOpsDone;
  }

  public int getTotalOpsDone() {
    return totalOpsDone;
  }

  public List<String> getBatchKeysList() {
    return batchKeysList;
  }

  public List<Set<String>> getBatchFieldsList() {
    return batchFieldsList;
  }

  public List<Map<String, ByteIterator>> getBatchValuesList() {
    return batchValuesList;
  }

  public boolean isWarmUpDone() {
    return warmupOpsDone >= warmupOpsCount;
  }

  public boolean isBatchPrepared(int batchSize) {
    int currentOpNum = totalOpsDone + 1;

    return getBatchKeysList().size() == batchSize
        || currentOpNum == warmupOpsCount
        || currentOpNum == totalOpsCount;
  }

  public CoreWorkloadThreadState incrementOps() {
    if (isWarmUpDone()) {
      payloadOpsDone++;
    } else {
      warmupOpsDone++;
    }
    totalOpsDone++;
    return this;
  }
}
