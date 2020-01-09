/*
 * Copyright 2018 YCSB Contributors. All Rights Reserved.
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

package site.ycsb.db.tablestore;

import java.util.*;
import java.util.function.*;

import site.ycsb.*;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.model.*;

import org.apache.log4j.Logger;

/**
 * TableStore Client for YCSB.
 */

public class TableStoreClient extends DB {

  private static SyncClient client;

  private int maxVersions = 1;
  private String primaryKeyName;

  private static final Logger LOGGER = Logger.getLogger(TableStoreClient.class);

  // nasty here as currently there is no support of JEP218
  private void setIntegerProperty(
      Properties properties,
      String propertyName,
      ClientConfiguration clientConfiguration,
      Function<Integer, Boolean> qualifyFunction,
      BiConsumer<ClientConfiguration, Integer> setFunction) throws DBException {
    String propertyString = properties.getProperty(propertyName);
    if (propertyString != null) {
      Integer propertyInteger = new Integer(propertyString);
      if (qualifyFunction.apply(propertyInteger).booleanValue()) {
        setFunction.accept(clientConfiguration, propertyInteger);
      } else {
        String errorMessage = "Illegal argument." + propertyName + ":" + propertyString;
        LOGGER.error(errorMessage);
        throw new DBException(errorMessage);
      }
    }
  }

  @Override
  public void init() throws DBException {

    Properties properties = getProperties();
    String accessID = properties.getProperty("alibaba.cloud.tablestore.access_id");
    String accessKey = properties.getProperty("alibaba.cloud.tablestore.access_key");
    String endPoint = properties.getProperty("alibaba.cloud.tablestore.end_point");
    String instanceName = properties.getProperty("alibaba.cloud.tablestore.instance_name");
    String maxVersion = properties.getProperty("alibaba.cloud.tablestore.max_version", "1");

    maxVersions = Integer.parseInt(maxVersion);
    primaryKeyName = properties.getProperty("alibaba.cloud.tablestore.primary_key", "");

    ClientConfiguration clientConfiguration = new ClientConfiguration();

    setIntegerProperty(
        properties,
        "alibaba.cloud.tablestore.connection_timeout",
        clientConfiguration,
        (Integer t) -> t > 0,
        (ClientConfiguration c, Integer t) -> c.setConnectionTimeoutInMillisecond(t.intValue()));

    setIntegerProperty(
        properties,
        "alibaba.cloud.tablestore.socket_timeout",
        clientConfiguration,
        (Integer t) -> t > 0,
        (ClientConfiguration c, Integer t) -> c.setSocketTimeoutInMillisecond(t.intValue()));

    setIntegerProperty(
        properties,
        "alibaba.cloud.tablestore.max_connections",
        clientConfiguration,
        (Integer t) -> t > 0,
        (ClientConfiguration c, Integer t) -> c.setMaxConnections(t.intValue()));

    try {
      synchronized (TableStoreClient.class) {
        if (client == null) {
          client = new SyncClient(endPoint, accessID, accessKey, instanceName, clientConfiguration);
          LOGGER.info("new tablestore sync client\tendpoint:" + endPoint + "\tinstanceName:" + instanceName);
        }
      }
    } catch (IllegalArgumentException e) {
      throw new DBException("Illegal argument passed in. Check the format of your parameters.", e);
    }
  }

  private void setResult(Set<String> fields, Map<String, ByteIterator> result, Row row) {
    if (row != null) {
      if (fields != null) {
        for (String field : fields) {
          result.put(field, new StringByteIterator((row.getColumn(field).toString())));
        }
      } else {
        for (Column column : row.getColumns()) {
          result.put(column.getName(), new StringByteIterator(column.getValue().asString()));
        }
      }
    }
  }

  private Status dealWithTableStoreException(TableStoreException e) {
    if (e.getErrorCode().contains("OTSRowOperationConflict")) {
      return Status.ERROR;
    }
    LOGGER.error(e);
    return Status.ERROR;
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      // set primary key
      PrimaryKeyColumn[] primaryKeyColumns = new PrimaryKeyColumn[1];
      primaryKeyColumns[0] = new PrimaryKeyColumn(primaryKeyName, PrimaryKeyValue.fromString(key));
      PrimaryKey primaryKey = new PrimaryKey(primaryKeyColumns);
      // set table_name
      SingleRowQueryCriteria singleRowQueryCriteria = new SingleRowQueryCriteria(table, primaryKey);
      singleRowQueryCriteria.setMaxVersions(maxVersions);
      // set columns
      if (fields != null) {
        singleRowQueryCriteria.addColumnsToGet(fields.toArray(new String[0]));
      }
      // set get_row request
      GetRowRequest getRowRequest = new GetRowRequest();
      getRowRequest.setRowQueryCriteria(singleRowQueryCriteria);
      // operate
      GetRowResponse getRowResponse = client.getRow(getRowRequest);
      // set the result
      setResult(fields, result, getRowResponse.getRow());
      return Status.OK;
    } catch (TableStoreException e) {
      return dealWithTableStoreException(e);
    } catch (Exception e) {
      LOGGER.error(e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    try {
      // set primary key
      PrimaryKeyColumn[] startKey = new PrimaryKeyColumn[1];
      startKey[0] = new PrimaryKeyColumn(primaryKeyName, PrimaryKeyValue.fromString(startkey));

      PrimaryKeyColumn[] endKey = new PrimaryKeyColumn[1];
      endKey[0] = new PrimaryKeyColumn(primaryKeyName, PrimaryKeyValue.INF_MAX);

      RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(table);
      criteria.setInclusiveStartPrimaryKey(new PrimaryKey(startKey));
      criteria.setExclusiveEndPrimaryKey(new PrimaryKey(endKey));
      criteria.setMaxVersions(maxVersions);
      // set columns
      if (fields != null) {
        criteria.addColumnsToGet(fields.toArray(new String[0]));
      }
      // set limit
      criteria.setLimit(recordcount);
      // set the request
      GetRangeRequest getRangeRequest = new GetRangeRequest();
      getRangeRequest.setRangeRowQueryCriteria(criteria);
      GetRangeResponse getRangeResponse = client.getRange(getRangeRequest);
      // set the result
      List<Row> rows = getRangeResponse.getRows();
      for (Row row : rows) {
        HashMap<String, ByteIterator> values = new HashMap<>();
        setResult(fields, values, row);
        result.add(values);
      }
      return Status.OK;
    } catch (TableStoreException e) {
      return dealWithTableStoreException(e);
    } catch (Exception e) {
      LOGGER.error(e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    try {
      PrimaryKeyColumn[] primaryKeyColumns = new PrimaryKeyColumn[1];
      primaryKeyColumns[0] = new PrimaryKeyColumn(primaryKeyName, PrimaryKeyValue.fromString(key));
      PrimaryKey primaryKey = new PrimaryKey(primaryKeyColumns);
      RowUpdateChange rowUpdateChange = new RowUpdateChange(table, primaryKey);

      for (Map.Entry<String, ByteIterator> entry: values.entrySet()) {
        rowUpdateChange.put(entry.getKey(), ColumnValue.fromString(entry.getValue().toString()));
      }

      UpdateRowRequest updateRowRequest = new UpdateRowRequest();
      updateRowRequest.setRowChange(rowUpdateChange);
      client.updateRow(updateRowRequest);
      return Status.OK;
    } catch (TableStoreException e) {
      return dealWithTableStoreException(e);
    } catch (Exception e) {
      LOGGER.error(e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      // set the primary key
      PrimaryKeyColumn[] primaryKeyColumns = new PrimaryKeyColumn[1];
      primaryKeyColumns[0] = new PrimaryKeyColumn(primaryKeyName, PrimaryKeyValue.fromString(key));
      PrimaryKey primaryKey = new PrimaryKey(primaryKeyColumns);
      RowPutChange rowPutChange = new RowPutChange(table, primaryKey);
      // set the columns
      for (Map.Entry<String, ByteIterator> entry: values.entrySet()) {
        rowPutChange.addColumn(entry.getKey(), ColumnValue.fromString(entry.getValue().toString()));
      }
      // set the putRow request
      PutRowRequest putRowRequest = new PutRowRequest();
      putRowRequest.setRowChange(rowPutChange);
      // operate
      client.putRow(putRowRequest);
      return Status.OK;
    } catch (TableStoreException e) {
      return dealWithTableStoreException(e);
    } catch (Exception e) {
      LOGGER.error(e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    try {
      PrimaryKeyColumn[] primaryKeyColumns = new PrimaryKeyColumn[1];
      primaryKeyColumns[0] = new PrimaryKeyColumn(primaryKeyName, PrimaryKeyValue.fromString(key));
      PrimaryKey primaryKey = new PrimaryKey(primaryKeyColumns);

      RowDeleteChange rowDeleteChange = new RowDeleteChange(table, primaryKey);

      DeleteRowRequest deleteRowRequest = new DeleteRowRequest();
      deleteRowRequest.setRowChange(rowDeleteChange);
      client.deleteRow(deleteRowRequest);
      return Status.OK;
    } catch (TableStoreException e) {
      return dealWithTableStoreException(e);
    } catch (Exception e) {
      LOGGER.error(e);
      return Status.ERROR;
    }
  }

}