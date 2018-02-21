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

import com.yahoo.ycsb.*;

import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.yahoo.ycsb.db.JdbcDBClientConstants.*;

/**
 * Hana / MonetDB / ??? YCSB-binding implementation class.
 */
public class JdbcDBClient extends TimeseriesDB {
  private static final String DEFAULT_PROP = "";
  private boolean initialized = false;
  private boolean doGroupBy = true;
  private Integer jdbcFetchSize;
  private Connection conn;
  private PreparedStatement insert = null;

  public void init() throws DBException {
    super.init();
    if (initialized) {
      System.out.println("Client connection already initialized.");
      return;
    }
    Properties props = getProperties();
    doGroupBy = Boolean.parseBoolean(props.getProperty("dogroupby", "true"));
    String url = props.getProperty(CONNECTION_URL, DEFAULT_PROP);
    String user = props.getProperty(CONNECTION_USER, DEFAULT_PROP);
    String passwd = props.getProperty(CONNECTION_PASSWD, DEFAULT_PROP);
    String driver = props.getProperty(DRIVER_CLASS);

    String jdbcFetchSizeStr = props.getProperty(JDBC_FETCH_SIZE);
    if (jdbcFetchSizeStr != null) {
      try {
        this.jdbcFetchSize = Integer.parseInt(jdbcFetchSizeStr);
      } catch (NumberFormatException nfe) {
        System.err.println("Invalid JDBC fetch size specified: " + jdbcFetchSizeStr);
        throw new DBException(nfe);
      }
    }

    String autoCommitStr = props.getProperty(JDBC_AUTO_COMMIT, Boolean.TRUE.toString());
    Boolean autoCommit = Boolean.parseBoolean(autoCommitStr);

    try {
      if (driver != null) {
        // load the JDBC driver class
        Class.forName(driver);
      }
      conn = DriverManager.getConnection(url, user, passwd);
      conn.setAutoCommit(autoCommit);
    } catch (ClassNotFoundException e) {
      System.err.println("Error in initializing the JDBS driver: " + e);
      throw new DBException(e);
    } catch (SQLException e) {
      System.err.println("Error in database operation: " + e);
      throw new DBException(e);
    } catch (NumberFormatException e) {
      System.err.println("Invalid value for fieldcount property. " + e);
      throw new DBException(e);
    }
    initialized = true;
  }

  @Override
  public void cleanup() throws DBException {
    super.cleanup();
    try {
      conn.close();
    } catch (SQLException e) {
      System.err.println("Error in closing the connection. " + e);
      throw new DBException(e);
    }
  }

  private void generateTagString(Map<String, List<String>> tags, StringBuilder sb) {
    int counter = 0;
    int tagCounter = 0;
    if (!tags.isEmpty()) {
      sb.append("(");
      for (String tag : tags.keySet()) {
        if (!tags.get(tag).isEmpty()) {
          sb.append("( ");
          for (String tagValue : tags.get(tag)) {
            sb.append(tag + " = " + "'" + tagValue + "' ");
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
      ResultSet resultSet = readStatement.executeQuery();
      if (!resultSet.next()) {
        resultSet.close();
        return Status.NOT_FOUND;
      }
      resultSet.close();
      return Status.OK;
    } catch (SQLException e) {
      System.err.println("ERROR: Error in processing read of table " + metric + ": " + e);
      return Status.ERROR;
    }
  }

  private PreparedStatement createReadStatement(String tableName, Map<String, List<String>> tags) throws SQLException {
    StringBuilder read = new StringBuilder("SELECT * FROM ");
    read.append(tableName);
    read.append(" WHERE ");
    generateTagString(tags, read);
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
    try {
      boolean ms = false;
      if (timeValue != 0) {
        if (TimeUnit.MILLISECONDS.convert(timeValue, timeUnit) == 1) {
          ms = true;
        } else {
          System.err.println("WARNING: JDBC does not support granularity, defaulting to one bucket.");
        }
      }
      PreparedStatement scanStatement = createScanStatement(metric, startTs, tags, aggreg, ms);
      scanStatement.setTimestamp(1, new Timestamp(startTs));
      scanStatement.setTimestamp(2, new Timestamp(endTs));
      ResultSet resultSet = scanStatement.executeQuery();
      if (!resultSet.next()) {
        resultSet.close();
        return Status.NOT_FOUND;
      }
      resultSet.close();
      return Status.OK;
    } catch (SQLException e) {
      System.err.println("ERROR: Error in processing scan of table: " + metric + e);
      return Status.ERROR;
    }
  }

  private PreparedStatement createScanStatement(String tableName, Long key, Map<String, List<String>> tags,
                                                AggregationOperation aggreg, boolean ms) throws SQLException {
    String selectStr = "*";
    String groupByStr = "";
    if (aggreg == AggregationOperation.AVERAGE) {
      if (!ms) {
        selectStr = "AVG(VALUE) as VALUE";
      } else {
        selectStr = TIMESTAMP_KEY + ", AVG(VALUE) as VALUE";
        if (doGroupBy) {
          groupByStr = "GROUP BY " + TIMESTAMP_KEY;
        }
      }
    } else if (aggreg == AggregationOperation.COUNT) {
      if (!ms) {
        selectStr = "COUNT(*) as VALUE";
      } else {
        selectStr = TIMESTAMP_KEY + ", COUNT(*) as VALUE";
        if (doGroupBy) {
          groupByStr = "GROUP BY " + TIMESTAMP_KEY;
        }
      }
    } else if (aggreg == AggregationOperation.SUM) {
      if (!ms) {
        selectStr = "SUM(VALUE) as VALUE";
      } else {
        selectStr = TIMESTAMP_KEY + ", SUM(VALUE) as VALUE";
        if (doGroupBy) {
          groupByStr = "GROUP BY " + TIMESTAMP_KEY;
        }
      }
    }

    StringBuilder select = new StringBuilder("SELECT " + selectStr + " FROM ");
    select.append(tableName);
    select.append(" WHERE ");
    generateTagString(tags, select);
    select.append(TIMESTAMP_KEY);
    select.append(" BETWEEN ? AND ? ");
    select.append(groupByStr + ";");
    PreparedStatement scanStatement = conn.prepareStatement(select.toString());
    if (this.jdbcFetchSize != null) {
      scanStatement.setFetchSize(this.jdbcFetchSize);
    }
    return scanStatement;
  }


  @Override
  public Status insert(String metric, Long timestamp, double value, Map<String, ByteIterator> tags) {
    if (metric == null || timestamp == null) {
      return Status.BAD_REQUEST;
    }
    try {
      int numFields = tags.size() + 2; // +PK +VALUE
      if (insert == null) {
        insert = createAndCacheInsertStatement(metric, tags, numFields);
      }
      PreparedStatement insertStatement = insert;
      insertStatement.setTimestamp(1, new Timestamp(timestamp));
      insertStatement.setDouble(2, value);
      int index = 3;
      for (Map.Entry<String, ByteIterator> entry : tags.entrySet()) {
        String tagvalue = entry.getValue().toString();
        insertStatement.setString(index++, tagvalue);
      }
      int result = insertStatement.executeUpdate();

      if (result == 1) {
        return Status.OK;
      } else {
        return Status.UNEXPECTED_STATE;
      }
    } catch (SQLException e) {
      System.err.println("Error in processing insert to table: " + metric + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }

  private PreparedStatement createAndCacheInsertStatement(String tableName, Map<String, ByteIterator> tags,
                                                          int numFields) throws SQLException {
    StringBuilder insertStatement = new StringBuilder("INSERT INTO ");
    insertStatement.append(tableName);
    insertStatement.append("(" + TIMESTAMP_KEY + ",VALUE");
    for (Map.Entry<String, ByteIterator> entry : tags.entrySet()) {
      insertStatement.append("," + entry.getKey().toString());
    }
    insertStatement.append(")");

    insertStatement.append(" VALUES(?");
    for (int i = 1; i < numFields; i++) {
      insertStatement.append(",?");
    }
    insertStatement.append(");");
    return conn.prepareStatement(insertStatement.toString());
  }
}
