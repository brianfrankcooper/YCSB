/**
 * Copyright (c) 2010 - 2016 Yahoo! Inc., 2016 YCSB contributors. All rights reserved.
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
package com.yahoo.ycsb.db;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A class that wraps a JDBC compliant database to allow it to be interfaced
 * with YCSB. This class extends {@link DB} and implements the database
 * interface used by YCSB client.
 *
 * <br>
 * Each client will have its own instance of this class. This client is not
 * thread safe.
 *
 * <br>
 * This interface expects a schema <key> <field1> <field2> <field3> ... All
 * attributes are of type VARCHAR. All accesses are through the primary key.
 * Therefore, only one index on the primary key is needed.
 */
public class JdbcDBClient extends DB {

  /** The class to use as the jdbc driver. */
  public static final String DRIVER_CLASS = "db.driver";

  /** The URL to connect to the database. */
  public static final String CONNECTION_URL = "db.url";

  /** The user name to use to connect to the database. */
  public static final String CONNECTION_USER = "db.user";

  /** The password to use for establishing the connection. */
  public static final String CONNECTION_PASSWD = "db.passwd";

  /** The batch size for batched inserts. Set to >0 to use batching */
  public static final String DB_BATCH_SIZE = "db.batchsize";

  /** The JDBC fetch size hinted to the driver. */
  public static final String JDBC_FETCH_SIZE = "jdbc.fetchsize";

  /** The JDBC connection auto-commit property for the driver. */
  public static final String JDBC_AUTO_COMMIT = "jdbc.autocommit";

  /** The name of the property for the number of fields in a record. */
  public static final String FIELD_COUNT_PROPERTY = "fieldcount";

  /** Default number of fields in a record. */
  public static final String FIELD_COUNT_PROPERTY_DEFAULT = "10";

  /** Representing a NULL value. */
  public static final String NULL_VALUE = "NULL";

  /** The primary key in the user table. */
  public static final String PRIMARY_KEY = "YCSB_KEY";

  /** The field name prefix in the table. */
  public static final String COLUMN_PREFIX = "FIELD";

  private ArrayList<Connection> conns;
  private boolean initialized = false;
  private Properties props;
  private int jdbcFetchSize;
  private int batchSize;
  private static final String DEFAULT_PROP = "";
  private ConcurrentMap<StatementType, PreparedStatement> cachedStatements;
  private long numRowsInBatch = 0;
  /** DB flavor defines DB-specific syntax and behavior for the
   * particular database. Current database flavors are: {default, phoenix} */
  private DBFlavor dbFlavor;

  /**
   * Ordered field information for insert and update statements.
   */
  private static class OrderedFieldInfo {
    private String fieldKeys;
    private List<String> fieldValues;

    OrderedFieldInfo(String fieldKeys, List<String> fieldValues) {
      this.fieldKeys = fieldKeys;
      this.fieldValues = fieldValues;
    }

    String getFieldKeys() {
      return fieldKeys;
    }

    List<String> getFieldValues() {
      return fieldValues;
    }
  }

  /**
   * DBFlavor captures minor differences in syntax and behavior among JDBC implementations and SQL
   * dialects.
   */
  private abstract static class DBFlavor {
    enum DBName {
      DEFAULT,
      PHOENIX
    }

    private final DBName dbName;

    public DBFlavor(DBName dbName) {
      this.dbName = dbName;
    }

    public static DBFlavor fromJdbcUrl(String url) {
      if (url.startsWith("jdbc:phoenix")) {
        return new PhoenixDBFlavor();
      }
      return new DefaultDBFlavor();
    }

    /**
     * Create and return a SQL statement for inserting data.
     */
    abstract String createInsertStatement(StatementType insertType, String key);

    /**
     * Create and return a SQL statement for reading data.
     */
    abstract String createReadStatement(StatementType readType, String key);

    /**
     * Create and return a SQL statement for deleting data.
     */
    abstract String createDeleteStatement(StatementType deleteType, String key);

    /**
     * Create and return a SQL statement for updating data.
     */
    abstract String createUpdateStatement(StatementType updateType, String key);

    /**
     * Create and return a SQL statement for scanning data.
     */
    abstract String createScanStatement(StatementType scanType, String key);
  }

  /**
   * The statement type for the prepared statements.
   */
  private static class StatementType {

    enum Type {
      INSERT(1), DELETE(2), READ(3), UPDATE(4), SCAN(5);

      private final int internalType;

      private Type(int type) {
        internalType = type;
      }

      int getHashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + internalType;
        return result;
      }
    }

    private Type type;
    private int shardIndex;
    private int numFields;
    private String tableName;
    private String fieldString;

    StatementType(Type type, String tableName, int numFields, String fieldString, int shardIndex) {
      this.type = type;
      this.tableName = tableName;
      this.numFields = numFields;
      this.fieldString = fieldString;
      this.shardIndex = shardIndex;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + numFields + 100 * shardIndex;
      result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
      result = prime * result + ((type == null) ? 0 : type.getHashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      StatementType other = (StatementType) obj;
      if (numFields != other.numFields) {
        return false;
      }
      if (shardIndex != other.shardIndex) {
        return false;
      }
      if (tableName == null) {
        if (other.tableName != null) {
          return false;
        }
      } else if (!tableName.equals(other.tableName)) {
        return false;
      }
      if (type != other.type) {
        return false;
      }
      if (!fieldString.equals(other.fieldString)) {
        return false;
      }
      return true;
    }
  }

  /**
   * For the given key, returns what shard contains data for this key.
   *
   * @param key Data key to do operation on
   * @return Shard index
   */
  private int getShardIndexByKey(String key) {
    int ret = Math.abs(key.hashCode()) % conns.size();
    return ret;
  }

  /**
   * For the given key, returns Connection object that holds connection to the
   * shard that contains this key.
   *
   * @param key Data key to get information for
   * @return Connection object
   */
  private Connection getShardConnectionByKey(String key) {
    return conns.get(getShardIndexByKey(key));
  }

  private void cleanupAllConnections() throws SQLException {
    for (Connection conn : conns) {
      conn.close();
    }
  }

  /** Returns parsed int value from the properties if set, otherwise returns -1. */
  private static int getIntProperty(Properties props, String key) throws DBException {
    String valueStr = props.getProperty(key);
    if (valueStr != null) {
      try {
        return Integer.parseInt(valueStr);
      } catch (NumberFormatException nfe) {
        System.err.println("Invalid " + key + " specified: " + valueStr);
        throw new DBException(nfe);
      }
    }
    return -1;
  }

  /** Returns parsed boolean value from the properties if set, otherwise returns defaultVal. */
  private static boolean getBoolProperty(Properties props, String key, boolean defaultVal) {
    String valueStr = props.getProperty(key);
    if (valueStr != null) {
      return Boolean.parseBoolean(valueStr);
    }
    return defaultVal;
  }

  @Override
  public void init() throws DBException {
    if (initialized) {
      System.err.println("Client connection already initialized.");
      return;
    }
    props = getProperties();
    String urls = props.getProperty(CONNECTION_URL, DEFAULT_PROP);
    String user = props.getProperty(CONNECTION_USER, DEFAULT_PROP);
    String passwd = props.getProperty(CONNECTION_PASSWD, DEFAULT_PROP);
    String driver = props.getProperty(DRIVER_CLASS);

    this.jdbcFetchSize = getIntProperty(props, JDBC_FETCH_SIZE);
    this.batchSize = getIntProperty(props, DB_BATCH_SIZE);

    boolean autoCommit = getBoolProperty(props, JDBC_AUTO_COMMIT, true);

    try {
      if (driver != null) {
        Class.forName(driver);
      }
      int shardCount = 0;
      conns = new ArrayList<Connection>(3);
      String[] urlArr = urls.split(",");
      for (String url : urlArr) {
        System.out.println("Adding shard node URL: " + url);
        Connection conn = DriverManager.getConnection(url, user, passwd);

        // Since there is no explicit commit method in the DB interface, all
        // operations should auto commit, except when explicitly told not to
        // (this is necessary in cases such as for PostgreSQL when running a
        // scan workload with fetchSize)
        conn.setAutoCommit(autoCommit);

        shardCount++;
        conns.add(conn);
      }

      System.out.println("Using shards: " + shardCount + ", batchSize:" + batchSize + ", fetchSize: " + jdbcFetchSize);

      cachedStatements = new ConcurrentHashMap<StatementType, PreparedStatement>();

      this.dbFlavor = DBFlavor.fromJdbcUrl(urlArr[0]);
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
    if (batchSize > 0) {
      try {
        // commit un-finished batches
        for (PreparedStatement st : cachedStatements.values()) {
          if (!st.getConnection().isClosed() && !st.isClosed() && (numRowsInBatch % batchSize != 0)) {
            st.executeBatch();
          }
        }
      } catch (SQLException e) {
        System.err.println("Error in cleanup execution. " + e);
        throw new DBException(e);
      }
    }

    try {
      cleanupAllConnections();
    } catch (SQLException e) {
      System.err.println("Error in closing the connection. " + e);
      throw new DBException(e);
    }
  }

  private PreparedStatement createAndCacheInsertStatement(StatementType insertType, String key)
      throws SQLException {
    String insert = dbFlavor.createInsertStatement(insertType, key);
    PreparedStatement insertStatement = getShardConnectionByKey(key).prepareStatement(insert);
    PreparedStatement stmt = cachedStatements.putIfAbsent(insertType, insertStatement);
    if (stmt == null) {
      return insertStatement;
    }
    return stmt;
  }

  private PreparedStatement createAndCacheReadStatement(StatementType readType, String key)
      throws SQLException {
    String read = dbFlavor.createReadStatement(readType, key);
    PreparedStatement readStatement = getShardConnectionByKey(key).prepareStatement(read);
    PreparedStatement stmt = cachedStatements.putIfAbsent(readType, readStatement);
    if (stmt == null) {
      return readStatement;
    }
    return stmt;
  }

  private PreparedStatement createAndCacheDeleteStatement(StatementType deleteType, String key)
      throws SQLException {
    String delete = dbFlavor.createDeleteStatement(deleteType, key);
    PreparedStatement deleteStatement = getShardConnectionByKey(key).prepareStatement(delete);
    PreparedStatement stmt = cachedStatements.putIfAbsent(deleteType, deleteStatement);
    if (stmt == null) {
      return deleteStatement;
    }
    return stmt;
  }

  private PreparedStatement createAndCacheUpdateStatement(StatementType updateType, String key)
      throws SQLException {
    String update = dbFlavor.createUpdateStatement(updateType, key);
    PreparedStatement insertStatement = getShardConnectionByKey(key).prepareStatement(update);
    PreparedStatement stmt = cachedStatements.putIfAbsent(updateType, insertStatement);
    if (stmt == null) {
      return insertStatement;
    }
    return stmt;
  }

  private PreparedStatement createAndCacheScanStatement(StatementType scanType, String key)
      throws SQLException {
    String select = dbFlavor.createScanStatement(scanType, key);
    PreparedStatement scanStatement = getShardConnectionByKey(key).prepareStatement(select);
    if (this.jdbcFetchSize > 0) {
      scanStatement.setFetchSize(this.jdbcFetchSize);
    }
    PreparedStatement stmt = cachedStatements.putIfAbsent(scanType, scanStatement);
    if (stmt == null) {
      return scanStatement;
    }
    return stmt;
  }

  private static class DefaultDBFlavor extends DBFlavor {
    public DefaultDBFlavor() {
      super(DBName.DEFAULT);
    }
    public DefaultDBFlavor(DBName dbName) {
      super(dbName);
    }

    @Override
    String createInsertStatement(StatementType insertType, String key) {
      StringBuilder insert = new StringBuilder("INSERT INTO ");
      insert.append(insertType.tableName);
      insert.append(" (" + PRIMARY_KEY + "," + insertType.fieldString + ")");
      insert.append(" VALUES(?");
      for (int i = 0; i < insertType.numFields; i++) {
        insert.append(",?");
      }
      insert.append(")");
      return insert.toString();
    }

    @Override
    String createReadStatement(StatementType readType, String key) {
      StringBuilder read = new StringBuilder("SELECT * FROM ");
      read.append(readType.tableName);
      read.append(" WHERE ");
      read.append(PRIMARY_KEY);
      read.append(" = ");
      read.append("?");
      return read.toString();
    }

    @Override
    String createDeleteStatement(StatementType deleteType, String key) {
      StringBuilder delete = new StringBuilder("DELETE FROM ");
      delete.append(deleteType.tableName);
      delete.append(" WHERE ");
      delete.append(PRIMARY_KEY);
      delete.append(" = ?");
      return delete.toString();
    }

    @Override
    String createUpdateStatement(StatementType updateType, String key) {
      String[] fieldKeys = updateType.fieldString.split(",");
      StringBuilder update = new StringBuilder("UPDATE ");
      update.append(updateType.tableName);
      update.append(" SET ");
      for (int i = 0; i < fieldKeys.length; i++) {
        update.append(fieldKeys[i]);
        update.append("=?");
        if (i < fieldKeys.length - 1) {
          update.append(", ");
        }
      }
      update.append(" WHERE ");
      update.append(PRIMARY_KEY);
      update.append(" = ?");
      return update.toString();
    }

    @Override
    String createScanStatement(StatementType scanType, String key) {
      StringBuilder select = new StringBuilder("SELECT * FROM ");
      select.append(scanType.tableName);
      select.append(" WHERE ");
      select.append(PRIMARY_KEY);
      select.append(" >= ?");
      select.append(" ORDER BY ");
      select.append(PRIMARY_KEY);
      select.append(" LIMIT ?");
      return select.toString();
    }
  }

  /**
   * Database flavor for Apache Phoenix. Captures syntax differences used by Phoenix.
   */
  private static class PhoenixDBFlavor extends DefaultDBFlavor {
    public PhoenixDBFlavor() {
      super(DBName.PHOENIX);
    }

    @Override
    String createInsertStatement(StatementType insertType, String key) {
      // Phoenix uses UPSERT syntax
      StringBuilder insert = new StringBuilder("UPSERT INTO ");
      insert.append(insertType.tableName);
      insert.append(" (" + PRIMARY_KEY + "," + insertType.fieldString + ")");
      insert.append(" VALUES(?");
      for (int i = 0; i < insertType.numFields; i++) {
        insert.append(",?");
      }
      insert.append(")");
      return insert.toString();
    }
  }

  @Override
  public Status read(String tableName, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    try {
      StatementType type = new StatementType(StatementType.Type.READ, tableName, 1, "", getShardIndexByKey(key));
      PreparedStatement readStatement = cachedStatements.get(type);
      if (readStatement == null) {
        readStatement = createAndCacheReadStatement(type, key);
      }
      readStatement.setString(1, key);
      ResultSet resultSet = readStatement.executeQuery();
      if (!resultSet.next()) {
        resultSet.close();
        return Status.NOT_FOUND;
      }
      if (result != null && fields != null) {
        for (String field : fields) {
          String value = resultSet.getString(field);
          result.put(field, new StringByteIterator(value));
        }
      }
      resultSet.close();
      return Status.OK;
    } catch (SQLException e) {
      System.err.println("Error in processing read of table " + tableName + ": " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String tableName, String startKey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    try {
      StatementType type = new StatementType(StatementType.Type.SCAN, tableName, 1, "", getShardIndexByKey(startKey));
      PreparedStatement scanStatement = cachedStatements.get(type);
      if (scanStatement == null) {
        scanStatement = createAndCacheScanStatement(type, startKey);
      }
      scanStatement.setString(1, startKey);
      scanStatement.setInt(2, recordcount);
      ResultSet resultSet = scanStatement.executeQuery();
      for (int i = 0; i < recordcount && resultSet.next(); i++) {
        if (result != null && fields != null) {
          HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
          for (String field : fields) {
            String value = resultSet.getString(field);
            values.put(field, new StringByteIterator(value));
          }
          result.add(values);
        }
      }
      resultSet.close();
      return Status.OK;
    } catch (SQLException e) {
      System.err.println("Error in processing scan of table: " + tableName + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String tableName, String key, HashMap<String, ByteIterator> values) {
    try {
      int numFields = values.size();
      OrderedFieldInfo fieldInfo = getFieldInfo(values);
      StatementType type = new StatementType(StatementType.Type.UPDATE, tableName,
          numFields, fieldInfo.getFieldKeys(), getShardIndexByKey(key));
      PreparedStatement updateStatement = cachedStatements.get(type);
      if (updateStatement == null) {
        updateStatement = createAndCacheUpdateStatement(type, key);
      }
      int index = 1;
      for (String value: fieldInfo.getFieldValues()) {
        updateStatement.setString(index++, value);
      }
      updateStatement.setString(index, key);
      int result = updateStatement.executeUpdate();
      if (result == 1) {
        return Status.OK;
      }
      return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      System.err.println("Error in processing update to table: " + tableName + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String tableName, String key, HashMap<String, ByteIterator> values) {
    try {
      int numFields = values.size();
      OrderedFieldInfo fieldInfo = getFieldInfo(values);
      StatementType type = new StatementType(StatementType.Type.INSERT, tableName,
          numFields, fieldInfo.getFieldKeys(), getShardIndexByKey(key));
      PreparedStatement insertStatement = cachedStatements.get(type);
      if (insertStatement == null) {
        insertStatement = createAndCacheInsertStatement(type, key);
      }
      insertStatement.setString(1, key);
      int index = 2;
      for (String value: fieldInfo.getFieldValues()) {
        insertStatement.setString(index++, value);
      }
      int result;
      if (batchSize > 0) {
        insertStatement.addBatch();
        if (++numRowsInBatch % batchSize == 0) {
          int[] results = insertStatement.executeBatch();
          for (int r : results) {
            if (r != 1) {
              return Status.ERROR;
            }
          }
          return Status.OK;
        }
        return Status.BATCHED_OK;
      } else {
        result = insertStatement.executeUpdate();
      }
      if (result == 1) {
        return Status.OK;
      }
      return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      System.err.println("Error in processing insert to table: " + tableName + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String tableName, String key) {
    try {
      StatementType type = new StatementType(StatementType.Type.DELETE, tableName, 1, "", getShardIndexByKey(key));
      PreparedStatement deleteStatement = cachedStatements.get(type);
      if (deleteStatement == null) {
        deleteStatement = createAndCacheDeleteStatement(type, key);
      }
      deleteStatement.setString(1, key);
      int result = deleteStatement.executeUpdate();
      if (result == 1) {
        return Status.OK;
      }
      return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      System.err.println("Error in processing delete to table: " + tableName + e);
      return Status.ERROR;
    }
  }

  private OrderedFieldInfo getFieldInfo(HashMap<String, ByteIterator> values) {
    String fieldKeys = "";
    List<String> fieldValues = new ArrayList();
    int count = 0;
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      fieldKeys += entry.getKey();
      if (count < values.size() - 1) {
        fieldKeys += ",";
      }
      fieldValues.add(count, entry.getValue().toString());
      count++;
    }

    return new OrderedFieldInfo(fieldKeys, fieldValues);
  }
}
