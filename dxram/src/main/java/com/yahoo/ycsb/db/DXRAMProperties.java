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

  enum DistributionStrategy {
    LINEAR("linear"),
    SCATTERED("scattered");

    private String string;

    DistributionStrategy(String string) {
      this.string = string;
    }

    public String toString() {
      return string;
    }

    public static DistributionStrategy fromString(String string) {
      if(LINEAR.string.startsWith(string)) {
        return LINEAR;
      } else if(SCATTERED.string.startsWith(string)) {
        return SCATTERED;
      }

      return LINEAR;
    }
  }

  private static final String NETWORK_TYPE_ETHERNET = "ethernet";
  private static final String NETWORK_TYPE_INFINIBAND = "infiniband";

  private static final String USE_POOLING = "dxram.pooling";
  private static final String DISTRIBUTION_STRATEGY = "dxram.distribution";
  private static final String BIND_ADDRESS = "dxram.bind";
  private static final String JOIN_ADDRESS = "dxram.join";
  private static final String NETWORK_TYPE = "dxram.network";

  private final int threadCount;
  private final int recordCount;
  private final int fieldsPerKey;
  private final int sizeOfField;

  private final boolean usePooling;
  private final DistributionStrategy distributionStrategy;
  private final IPV4Unit bindAddress;
  private final IPV4Unit joinAddress;
  private final String networkType;

  DXRAMProperties(final Properties properties) {
    threadCount = Integer.parseInt(properties.getProperty(Client.THREAD_COUNT_PROPERTY, "1"));

    recordCount = Integer.parseInt(properties.getProperty(Client.RECORD_COUNT_PROPERTY, "-1"));
    checkParameter(Client.RECORD_COUNT_PROPERTY, recordCount);

    fieldsPerKey = Integer.parseInt(properties.getProperty(CoreWorkload.FIELD_COUNT_PROPERTY, "-1"));
    checkParameter(CoreWorkload.FIELD_COUNT_PROPERTY, fieldsPerKey);

    sizeOfField = Integer.parseInt(properties.getProperty(CoreWorkload.FIELD_LENGTH_PROPERTY, "-1"));
    checkParameter(CoreWorkload.FIELD_LENGTH_PROPERTY, sizeOfField);

    usePooling = Boolean.parseBoolean(properties.getProperty(USE_POOLING, "true"));

    distributionStrategy = DistributionStrategy.fromString(
        properties.getProperty(DISTRIBUTION_STRATEGY, DistributionStrategy.LINEAR.toString()));

    String bind = properties.getProperty(BIND_ADDRESS, "-1");
    checkParameter(BIND_ADDRESS, bind);

    String join = properties.getProperty(JOIN_ADDRESS, "-1");
    checkParameter(JOIN_ADDRESS, bind);

    String[] bindArr = bind.split(":");
    bindAddress = new IPV4Unit(bindArr[0], Integer.parseInt(bindArr[1]));

    String[] joinArr = join.split(":");
    joinAddress = new IPV4Unit(joinArr[0], Integer.parseInt(joinArr[1]));

    String tmpNetworkType = properties.getProperty(NETWORK_TYPE, "ethernet");

    if(NETWORK_TYPE_ETHERNET.startsWith(tmpNetworkType)) {
      tmpNetworkType = NETWORK_TYPE_ETHERNET;
    } else if(NETWORK_TYPE_INFINIBAND.startsWith(tmpNetworkType) || tmpNetworkType.equals("ib")) {
      tmpNetworkType = NETWORK_TYPE_INFINIBAND;
    }

    networkType = tmpNetworkType;
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

  String getNetworkType() {
    return networkType;
  }

  boolean usePooling() {
    return usePooling;
  }

  DistributionStrategy getDistributionStrategy() {
    return distributionStrategy;
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
