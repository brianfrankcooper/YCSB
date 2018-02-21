/*
 * Copyright (c) 2015 - 2018 Anita Armbruster, Andreas Bader, Patrick Dabbert, Pascal Litty,
 *               2018 YCSB Contributors All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.TimeseriesDB;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


/**
 * Hana / MonetDB / ??? YCSB-binding implementation class.
 */
public class HanaClient extends TimeseriesDB {

  static final String DRIVER_CLASS_PROPERTY = "db.driver";

  static final String CONNECTION_URL_PROPERTY = "db.url";
  static final String CONNECTION_USER_PROPERTY = "db.user";
  static final String CONNECTION_PASSWORD_PROPERTY = "db.passwd";
  private static final String CONNECTION_PASSWORD_PROPERTY_DEFAULT = "";

  private static final String JDBC_FETCH_SIZE_PROPERTY = "jdbc.fetchsize";
  private static final String JDBC_AUTO_COMMIT_PROPERTY = "jdbc.autocommit";

  private static final String DO_GROUP_BY_PROPERTY = "dogroupby";
  private static final String DO_GROUP_BY_PROERTY_DEFAULT = "true";

  static final String FIELD_COUNT_PROPERTY = "fieldcount";
  static final String FIELD_COUNT_PROPERTY_DEFAULT = "10";

  private static final String TIMESTAMP_KEY = "YCSB_KEY";
  private static final String JDBC_AUTO_COMMIT_PROPERTY_DEFAULT = "true";

  private boolean initialized = false;
  private boolean doGroupBy = true;
  private int fieldCount;
  private Integer jdbcFetchSize;
  private Connection conn;

  @Override
  public void init() throws DBException {
    super.init();
    // When would this ever happen??
    if (initialized) {
      LOGGER.info("Client connection already initialized.");
      return;
    }
    Properties props = getProperties();
    if (!props.containsKey(CONNECTION_URL_PROPERTY)) {
      throwMissingProperty(CONNECTION_URL_PROPERTY);
    }
    if (!props.containsKey(CONNECTION_USER_PROPERTY)) {
      throwMissingProperty(CONNECTION_USER_PROPERTY);
    }
    if (!props.containsKey(DRIVER_CLASS_PROPERTY)) {
      throwMissingProperty(DRIVER_CLASS_PROPERTY);
    }
    String driver = props.getProperty(DRIVER_CLASS_PROPERTY);
    String url = props.getProperty(CONNECTION_URL_PROPERTY);
    String user = props.getProperty(CONNECTION_USER_PROPERTY);
    String passwd = props.getProperty(CONNECTION_PASSWORD_PROPERTY, CONNECTION_PASSWORD_PROPERTY_DEFAULT);
    doGroupBy = Boolean.parseBoolean(props.getProperty(DO_GROUP_BY_PROPERTY, DO_GROUP_BY_PROERTY_DEFAULT));
    fieldCount = Integer.parseInt(props.getProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_PROPERTY_DEFAULT));

    String jdbcFetchSizeStr = props.getProperty(JDBC_FETCH_SIZE_PROPERTY);
    if (jdbcFetchSizeStr != null && !jdbcFetchSizeStr.isEmpty()) {
      try {
        this.jdbcFetchSize = Integer.parseInt(jdbcFetchSizeStr);
      } catch (NumberFormatException nfe) {
        LOGGER.error("Invalid JDBC fetch size specified: " + jdbcFetchSizeStr);
        throw new DBException(nfe);
      }
    }

    Boolean autoCommit = Boolean.parseBoolean(props.getProperty(JDBC_AUTO_COMMIT_PROPERTY,
        JDBC_AUTO_COMMIT_PROPERTY_DEFAULT));

    try {
      if (driver != null) {
        // load the JDBC driver class
        Class.forName(driver);
      }
      conn = DriverManager.getConnection(url, user, passwd);
      conn.setAutoCommit(autoCommit);
      // drop and create table usermetric
      // FIXME allow setting tablename
      createTable("usermetric");
    } catch (ClassNotFoundException e) {
      LOGGER.error("Error in initializing the JDBC driver: " + e);
      throw new DBException(e);
    } catch (SQLException e) {
      LOGGER.error("Error in database operation: " + e);
      throw new DBException(e);
    } catch (NumberFormatException e) {
      LOGGER.error("Invalid value for fieldcount property. " + e);
      throw new DBException(e);
    }
    initialized = true;
  }

  private void createTable(String tablename) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      StringBuilder sql = new StringBuilder("DROP TABLE IF EXISTS ");
      sql.append(tablename);
      sql.append(";");

      stmt.execute(sql.toString());
      LOGGER.info("Successfully dropped table {}.", tablename);
      sql = new StringBuilder("CREATE TABLE ");
      sql.append(tablename);
      sql.append(" (KEY VARCHAR PRIMARY KEY");

      for (int idx = 0; idx < fieldCount; idx++) {
        sql.append(", FIELD");
        sql.append(idx);
        sql.append(" VARCHAR");
      }
      sql.append(");");

      stmt.execute(sql.toString());

      LOGGER.info("Successfully created table {} with {} fields", tablename, fieldCount);
    }
  }

  @Override
  public void cleanup() throws DBException {
    super.cleanup();
    try {
      conn.close();
    } catch (SQLException e) {
      LOGGER.error("Error in closing the connection. " + e);
      throw new DBException(e);
    }
  }

  private void appendTagClauses(Map<String, List<String>> tags, StringBuilder sb) {
    int counter = 0;
    int tagCounter = 0;
    if (!tags.isEmpty()) {
      sb.append("(");
      for (Map.Entry<String, List<String>> specification : tags.entrySet()) {
        if (specification.getValue().isEmpty()) {
          continue;
        }
        sb.append("( ");
        for (String tagValue : specification.getValue()) {
          sb.append(specification.getKey()).append(" = ").append("'").append(tagValue).append("' ");
          sb.append("OR ");
          counter++;
        }
        sb.delete(sb.lastIndexOf("OR"), sb.lastIndexOf("OR") + 2);
        sb.append(") ");
        // If its the last one...
        if (tagCounter < tags.keySet().size() - 1) {
          sb.append("AND ");
        }
        tagCounter++;
      }
      sb.append(")");
    }
    if (counter > 0) {
      sb.append(" AND ");
    }
  }

  @Override
  public Status read(String metric, Long timestamp, Map<String, List<String>> tags) {
    if (metric == null || timestamp == null) {
      return Status.BAD_REQUEST;
    }
    try {
      PreparedStatement readStatement = createReadStatement(metric, tags);
      readStatement.setTimestamp(1, new Timestamp(timestamp));
      try (ResultSet resultSet = readStatement.executeQuery()) {
        if (!resultSet.next()) {
          return Status.NOT_FOUND;
        }
      }
      return Status.OK;
    } catch (SQLException e) {
      LOGGER.error("Error in processing read of table " + metric + ": " + e);
      return Status.ERROR;
    }
  }

  private PreparedStatement createReadStatement(String tableName, Map<String, List<String>> tags) throws SQLException {
    StringBuilder read = new StringBuilder("SELECT * FROM ");
    read.append(tableName);
    read.append(" WHERE ");
    appendTagClauses(tags, read);
    read.append(" ( ");
    read.append(TIMESTAMP_KEY);
    read.append(" = ");
    read.append("?);");
    return conn.prepareStatement(read.toString());
  }

  @Override
  public Status scan(String metric, Long startTs, Long endTs, Map<String, List<String>> tags,
                     AggregationOperation aggreg, int timeValue, TimeUnit timeUnit) {
    if (metric == null || startTs == null || endTs == null) {
      return Status.BAD_REQUEST;
    }
    boolean ms = false;
    if (timeValue != 0) {
      if (TimeUnit.MILLISECONDS.convert(timeValue, timeUnit) == 1) {
        ms = true;
      } else {
        LOGGER.warn("JDBC does not support granularity, defaulting to one bucket.");
      }
    }
    final String queryText = createScanStatement(metric, tags, aggreg, ms);
    try (PreparedStatement scanStatement = conn.prepareStatement(queryText)) {
      if (this.jdbcFetchSize != null) {
        scanStatement.setFetchSize(this.jdbcFetchSize);
      }
      scanStatement.setTimestamp(1, new Timestamp(startTs));
      scanStatement.setTimestamp(2, new Timestamp(endTs));
      try (ResultSet resultSet = scanStatement.executeQuery()) {
        if (!resultSet.next()) {
          return Status.NOT_FOUND;
        }
      }
      return Status.OK;
    } catch (SQLException e) {
      LOGGER.error("Error in processing scan of table: " + metric + e);
      return Status.ERROR;
    }

  }

  private String createScanStatement(String tableName, Map<String, List<String>> tags,
                                     AggregationOperation aggreg, boolean ms) {
    final String selectStr;
    final String groupByStr = ms && doGroupBy ? "GROUP BY " + TIMESTAMP_KEY : "";
    switch (aggreg) {
    case AVERAGE:
      if (ms) {
        selectStr = TIMESTAMP_KEY + ", AVG(VALUE) as VALUE";
      } else {
        selectStr = "AVG(VALUE) as VALUE";
      }
      break;
    case COUNT:
      if (ms) {
        selectStr = TIMESTAMP_KEY + ", COUNT(*) as VALUE";
      } else {
        selectStr = "COUNT(*) as VALUE";
      }
      break;
    case SUM:
      if (ms) {
        selectStr = TIMESTAMP_KEY + ", SUM(VALUE) as VALUE";
      } else {
        selectStr = "SUM(VALUE) as VALUE";
      }
      break;
    case NONE:
    default:
      selectStr = "*";
      break;
    }
    StringBuilder select = new StringBuilder("SELECT " + selectStr + " FROM ");
    select.append(tableName);
    select.append(" WHERE ");
    appendTagClauses(tags, select);
    select.append(TIMESTAMP_KEY);
    select.append(" BETWEEN ? AND ? ");
    select.append(groupByStr).append(";");
    return select.toString();
  }


  @Override
  public Status insert(String metric, Long timestamp, double value, Map<String, ByteIterator> tags) {
    if (metric == null || timestamp == null) {
      return Status.BAD_REQUEST;
    }
    // this helps guarantee iteration order for labels and values when preparing the statement.
    List<Map.Entry<String, ByteIterator>> orderedTags = new ArrayList<>(tags.entrySet());
    try (PreparedStatement insertStatement = prepareInsert(metric, orderedTags)) {
      insertStatement.setTimestamp(1, new Timestamp(timestamp));
      insertStatement.setDouble(2, value);
      int index = 3;
      for (Map.Entry<String, ByteIterator> entry : orderedTags) {
        String tagvalue = entry.getValue().toString();
        insertStatement.setString(index++, tagvalue);
      }
      int result = insertStatement.executeUpdate();
      return result == 1 ? Status.OK : Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      LOGGER.error("Error in processing insert to table: " + metric + e);
      return Status.ERROR;
    }
  }

  private PreparedStatement prepareInsert(String tableName, List<Map.Entry<String, ByteIterator>> tags)
      throws SQLException {
    StringBuilder insertStatement = new StringBuilder("INSERT INTO ");
    insertStatement.append(tableName);
    insertStatement.append("(" + TIMESTAMP_KEY + ",VALUE");
    for (Map.Entry<String, ByteIterator> entry : tags) {
      insertStatement.append(",").append(entry.getKey());
    }
    insertStatement.append(")");
    insertStatement.append(" VALUES(?");

    // tags + PK + Value
    for (int i = 1; i < tags.size() + 2; i++) {
      insertStatement.append(",?");
    }
    insertStatement.append(");");
    return conn.prepareStatement(insertStatement.toString());
  }

  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }
}
