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
  private static final String TOTAL_RECORD_COUNT = "dxram.recordCount";

  private final int threadCount;
  private final int recordCount;
  private final int fieldsPerKey;
  private final int sizeOfField;

  DXRAMProperties(final Properties properties) {
    threadCount = Integer.parseInt(properties.getProperty(Client.THREAD_COUNT_PROPERTY));
    recordCount = Integer.parseInt(properties.getProperty(Client.RECORD_COUNT_PROPERTY));
    fieldsPerKey = Integer.parseInt(properties.getProperty(CoreWorkload.FIELD_COUNT_PROPERTY));
    sizeOfField = Integer.parseInt(properties.getProperty(CoreWorkload.FIELD_LENGTH_PROPERTY));
  }

  int getThreadCount() {
    return threadCount;
  }

  int getFieldsPerKey() {
    return fieldsPerKey;
  }

  int getSizeOfField() {
    return sizeOfField;
  }

  int getRecordCount() {
    return recordCount;
  }

  private void checkParameter(final String name, final int val) {
    if (val == -1) {
      throw new IllegalArgumentException("Required parameter not specified: " + name);
    }
  }
}
