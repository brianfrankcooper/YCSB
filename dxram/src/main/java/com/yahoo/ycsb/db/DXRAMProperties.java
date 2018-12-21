package com.yahoo.ycsb.db;

import java.util.Properties;

import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.workloads.CoreWorkload;

/**
 * Wrapper class to get properties for the DXRAMClient.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 09.11.2018
 */
class DXRAMProperties {
  private static final String STORE_NODES = "dxram.stores";
  private static final String TARGET_LOAD_NODE_IDX = "dxram.load.targetNodeIdx";
  private static final String RECORDS_PER_STORE_NODE = "dxram.recordsPerStoreNode";

  private final int threadCount;
  private final int fieldsPerKey;
  private final int sizeOfField;

  // both ycsb types
  private final int storageNodeCount;

  // loader only
  private final int targetLoadNodeIdx;

  // benchmark only
  private final int recordsPerNode;

  DXRAMProperties(final Properties properties) {
    threadCount = Integer.parseInt(properties.getProperty(Client.THREAD_COUNT_PROPERTY));
    fieldsPerKey = Integer.parseInt(properties.getProperty(CoreWorkload.FIELD_COUNT_PROPERTY));
    sizeOfField = Integer.parseInt(properties.getProperty(CoreWorkload.FIELD_LENGTH_PROPERTY));

    storageNodeCount = Integer.parseInt(properties.getProperty(STORE_NODES, "-1"));
    checkParameter(STORE_NODES, storageNodeCount);

    targetLoadNodeIdx = Integer.parseInt(properties.getProperty(TARGET_LOAD_NODE_IDX, "-1"));

    recordsPerNode = Integer.parseInt(properties.getProperty(RECORDS_PER_STORE_NODE, "-1"));
    checkParameter(STORE_NODES, storageNodeCount);
  }

  int getThreadCount() {
    return threadCount;
  }

  int getStorageNodeCount() {
    return storageNodeCount;
  }

  int getFieldsPerKey() {
    return fieldsPerKey;
  }

  int getSizeOfField() {
    return sizeOfField;
  }

  int getTargetLoadNodeIdx() {
    return targetLoadNodeIdx;
  }

  int getRecordsPerNode() {
    return recordsPerNode;
  }

  private void checkParameter(final String name, final int val) {
    if (val == -1) {
      throw new IllegalArgumentException("Required parameter not specified: " + name);
    }
  }
}
