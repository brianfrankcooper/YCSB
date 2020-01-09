/**
 * Copyright (c) 2018 TOSHIBA Digital Solutions Corporation.
 * Copyright (c) 2018 YCSB contributors.
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
 
package site.ycsb.db.griddb;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.logging.Logger;

import com.toshiba.mwcloud.gs.ColumnInfo;
import com.toshiba.mwcloud.gs.Container;
import com.toshiba.mwcloud.gs.ContainerInfo;
import com.toshiba.mwcloud.gs.ContainerType;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GSType;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.PartitionController;
import com.toshiba.mwcloud.gs.Row;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;

/**
 * A class representing GridDBClient.
 *
 */
public class GridDBClient extends site.ycsb.DB {
  //(A)multicast method
  private String notificationAddress = ""; // "239.0.0.1"
  private String notificationPort = ""; // "31999"
  //(B)fixed list method
  private String notificationMember = ""; // "10.0.0.12:10001,10.0.0.13:10001,10.0.0.14:10001"

  private String userName = ""; //
  private String password = ""; //
  private String clusterName = ""; // "ycsbCluster"


  public static final String VALUE_COLUMN_NAME_PREFIX= "field";
  public static final int ROW_KEY_COLUMN_POS = 0;

  private String containerPrefix = "";

  public static final int DEFAULT_CACHE_CONTAINER_NUM = 1000;
  public static final int FIELD_NUM = 10;

  private int numContainer = 0; // Sets PartitionNum
  public static final GSType SCHEMA_TYPE = GSType.STRING;

  private GridStore store;
  private ContainerInfo containerInfo = null;
  private static final Logger LOGGER = Logger.getLogger(GridDBClient.class.getName());

  class RowComparator implements Comparator<Row> {
    public int compare(Row row1, Row row2) throws NullPointerException {
      int result = 0;
      try {
        Object val1 = row1.getValue(0);
        Object val2 = row2.getValue(0);
        result = ((String)val1).compareTo((String)val2);
      } catch (GSException e) {
        LOGGER.severe("There is a exception: " + e.getMessage());
        throw new NullPointerException();
      }
      return result;
    }
  }

  public void init() throws DBException {
    LOGGER.info("GridDBClient");

    final Properties props = getProperties();
    notificationAddress = props.getProperty("notificationAddress");
    notificationPort = props.getProperty("notificationPort");
    notificationMember = props.getProperty("notificationMember");
    clusterName = props.getProperty("clusterName");
    userName = props.getProperty("userName");
    password = props.getProperty("password");
    containerPrefix = props.getProperty("table", "usertable") + "@";
    String fieldcount = props.getProperty("fieldcount");
    String fieldlength = props.getProperty("fieldlength");

    LOGGER.info("notificationAddress=" + notificationAddress + " notificationPort=" + notificationPort +
            " notificationMember=" + notificationMember);
    LOGGER.info("clusterName=" + clusterName + " userName=" + userName);
    LOGGER.info("fieldcount=" + fieldcount + " fieldlength=" + fieldlength);


    final Properties gridstoreProp = new Properties();
    if (clusterName == null || userName == null || password == null) {
      LOGGER.severe("[ERROR] clusterName or userName or password argument not specified");
      throw new DBException();
    }
    if (fieldcount == null || fieldlength == null) {
      LOGGER.severe("[ERROR] fieldcount or fieldlength argument not specified");
      throw new DBException();
    } else {
      if (!fieldcount.equals(String.valueOf(FIELD_NUM)) || !fieldlength.equals("100")) {
        LOGGER.severe("[ERROR] Invalid argment: fieldcount or fieldlength");
        throw new DBException();
      }
    }
    if (notificationAddress != null) {
      if (notificationPort == null) {
        LOGGER.severe("[ERROR] notificationPort argument not specified");
        throw new DBException();
      }
      //(A)multicast method
      gridstoreProp.setProperty("notificationAddress", notificationAddress);
      gridstoreProp.setProperty("notificationPort", notificationPort);
    } else if (notificationMember != null) {
      //(B)fixed list method
      gridstoreProp.setProperty("notificationMember", notificationMember);
    } else {
      LOGGER.severe("[ERROR] notificationAddress and notificationMember argument not specified");
      throw new DBException();
    }
    gridstoreProp.setProperty("clusterName", clusterName);
    gridstoreProp.setProperty("user", userName);
    gridstoreProp.setProperty("password", password);

    gridstoreProp.setProperty("containerCacheSize", String.valueOf(DEFAULT_CACHE_CONTAINER_NUM));

    List<ColumnInfo> columnInfoList = new ArrayList<ColumnInfo>();
    ColumnInfo keyInfo = new ColumnInfo("key", SCHEMA_TYPE);
    columnInfoList.add(keyInfo);
    for (int i = 0; i < FIELD_NUM; i++) {
      String columnName = String.format(VALUE_COLUMN_NAME_PREFIX + "%d", i);
      ColumnInfo info = new ColumnInfo(columnName, SCHEMA_TYPE);
      columnInfoList.add(info);
    }
    containerInfo = new ContainerInfo(null, ContainerType.COLLECTION, columnInfoList, true);

    try {
      GridStoreFactory.getInstance().setProperties(gridstoreProp);
      store = GridStoreFactory.getInstance().getGridStore(gridstoreProp);
      PartitionController controller = store.getPartitionController();
      numContainer = controller.getPartitionCount();

      for(int k = 0; k < numContainer; k++) {
        String name = containerPrefix + k;
        store.putContainer(name, containerInfo, false);
      }
    } catch (GSException e) {
      LOGGER.severe("Exception: " + e.getMessage());
      throw new DBException();
    }

    LOGGER.info("numContainer=" + numContainer + " containerCasheSize=" + 
            String.valueOf(DEFAULT_CACHE_CONTAINER_NUM));

  }

  public void cleanup() throws DBException {
    try {
      store.close();
    } catch (GSException e) {
      LOGGER.severe("Exception when close." + e.getMessage());
      throw new DBException();
    }
  }

  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {

      Object rowKey = makeRowKey(key);
      String containerKey = makeContainerKey(key);

      final Container<Object, Row> container = store.getContainer(containerKey);
      if(container == null) {
        LOGGER.severe("[ERROR]getCollection " + containerKey + " in read()");
        return Status.ERROR;
      }

      Row targetRow = container.get(rowKey);
      if (targetRow == null) {
        LOGGER.severe("[ERROR]get(rowKey) in read()");
        return Status.ERROR;
      }

      for (int i = 1; i < containerInfo.getColumnCount(); i++) {
        result.put(containerInfo.getColumnInfo(i).getName(),
            new ByteArrayByteIterator(targetRow.getValue(i).toString().getBytes()));
      }

      return Status.OK;

    } catch (GSException e) {
      LOGGER.severe("Exception: " + e.getMessage());
      return Status.ERROR;
    }
  }

  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
          Vector<HashMap<String, ByteIterator>> result) {
    LOGGER.severe("[ERROR]scan() not supported");
    return Status.ERROR;
  }

  public Status update(String table, String key, Map<String, ByteIterator> values) {
    try {
      Object rowKey = makeRowKey(key);
      String containerKey = makeContainerKey(key);

      final Container<Object, Row> container = store.getContainer(containerKey);
      if(container == null) {
        LOGGER.severe("[ERROR]getCollection " + containerKey + " in update()");
        return Status.ERROR;
      }

      Row targetRow = container.get(rowKey);
      if (targetRow == null) {
        LOGGER.severe("[ERROR]get(rowKey) in update()");
        return Status.ERROR;
      }

      int setCount = 0;
      for (int i = 1; i < containerInfo.getColumnCount() && setCount < values.size(); i++) {
        containerInfo.getColumnInfo(i).getName();
        ByteIterator byteIterator = values.get(containerInfo.getColumnInfo(i).getName());
        if (byteIterator != null) {
          Object value = makeValue(byteIterator);
          targetRow.setValue(i, value);
          setCount++;
        }
      }
      if (setCount != values.size()) {
        LOGGER.severe("Error setCount = " + setCount);
        return Status.ERROR;
      }

      container.put(targetRow);

      return Status.OK;
    } catch (GSException e) {
      LOGGER.severe("Exception: " + e.getMessage());
      return Status.ERROR;
    }
  }

  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {

      Object rowKey = makeRowKey(key);
      String containerKey = makeContainerKey(key);

      final Container<Object, Row> container = store.getContainer(containerKey);
      if(container == null) {
        LOGGER.severe("[ERROR]getCollection " + containerKey + " in insert()");
      }

      Row row = container.createRow();

      row.setValue(ROW_KEY_COLUMN_POS, rowKey);

      for (int i = 1; i < containerInfo.getColumnCount(); i++) {
        ByteIterator byteIterator = values.get(containerInfo.getColumnInfo(i).getName());
        Object value = makeValue(byteIterator);
        row.setValue(i, value);
      }

      container.put(row);

    } catch (GSException e) {
      LOGGER.severe("Exception: " + e.getMessage());
      return Status.ERROR;
    }

    return Status.OK;
  }

  public Status delete(String table, String key) {
    try {
      Object rowKey = makeRowKey(key);
      String containerKey = makeContainerKey(key);

      final Container<Object, Row> container = store.getContainer(containerKey);
      if(container == null) {
        LOGGER.severe("[ERROR]getCollection " + containerKey + " in read()");
        return Status.ERROR;
      }

      boolean isDelete = container.remove(rowKey);
      if (!isDelete) {
        LOGGER.severe("[ERROR]remove(rowKey) in remove()");
        return Status.ERROR;
      }
    }catch (GSException e) {
      LOGGER.severe("Exception: " + e.getMessage());
      return Status.ERROR;
    }
    return Status.OK;
  }

  protected String makeContainerKey(String key) {
    return containerPrefix + Math.abs(key.hashCode() % numContainer);
  }

  protected Object makeRowKey(String key) {
    return key;
  }

  protected Object makeValue(ByteIterator byteIterator) {
    return byteIterator.toString();
  }

  protected Object makeQueryLiteral(Object value) {
    return "'" + value.toString() + "'";
  }
}
