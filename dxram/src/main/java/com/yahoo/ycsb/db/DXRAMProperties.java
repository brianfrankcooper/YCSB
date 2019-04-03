package com.yahoo.ycsb.db;

import java.util.Properties;

import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.workloads.CoreWorkload;
import de.hhu.bsinfo.dxutils.unit.IPV4Unit;

/**
 * Wrapper class to get properties for the DXRAMClient.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 09.11.2018
 */
class DXRAMProperties {
  private static final String BIND_ADDRESS = "dxram.bind";
  private static final String JOIN_ADDRESS = "dxram.join";

  private final int threadCount;
  private final int recordCount;
  private final int fieldsPerKey;
  private final int sizeOfField;

  private final IPV4Unit bindAddress;
  private final IPV4Unit joinAddress;

  DXRAMProperties(final Properties properties) {
    threadCount = Integer.parseInt(properties.getProperty(Client.THREAD_COUNT_PROPERTY, "-1"));
    checkParameter(Client.THREAD_COUNT_PROPERTY, threadCount);

    recordCount = Integer.parseInt(properties.getProperty(Client.RECORD_COUNT_PROPERTY, "-1"));
    checkParameter(Client.RECORD_COUNT_PROPERTY, recordCount);

    fieldsPerKey = Integer.parseInt(properties.getProperty(CoreWorkload.FIELD_COUNT_PROPERTY, "-1"));
    checkParameter(CoreWorkload.FIELD_COUNT_PROPERTY, fieldsPerKey);

    sizeOfField = Integer.parseInt(properties.getProperty(CoreWorkload.FIELD_LENGTH_PROPERTY, "-1"));
    checkParameter(CoreWorkload.FIELD_LENGTH_PROPERTY, sizeOfField);

    String bind = properties.getProperty(BIND_ADDRESS, "-1");
    checkParameter(BIND_ADDRESS, bind);

    String join = properties.getProperty(JOIN_ADDRESS, "-1");
    checkParameter(JOIN_ADDRESS, bind);

    String[] bindArr = bind.split(":");
    bindAddress = new IPV4Unit(bindArr[0], Integer.parseInt(bindArr[1]));

    String[] joinArr = join.split(":");
    joinAddress = new IPV4Unit(joinArr[0], Integer.parseInt(joinArr[1]));
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

  IPV4Unit getBindAddress() {
    return bindAddress;
  }

  IPV4Unit getJoinAddress() {
    return joinAddress;
  }

  private void checkParameter(final String name, final int val) {
    if (val == -1) {
      throw new IllegalArgumentException("Required parameter not specified: " + name);
    }
  }

  private void checkParameter(final String name, final String val) {
    if (val.equals("-1")) {
      throw new IllegalArgumentException("Required parameter not specified: " + name);
    }
  }
}
