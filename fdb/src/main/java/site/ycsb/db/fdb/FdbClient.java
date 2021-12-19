/**
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

package site.ycsb.db.fdb;

import com.light.fdb.TableName;
import com.light.fdb.client.*;
import com.light.fdb.util.Bytes;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.measurements.Measurements;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static site.ycsb.workloads.CoreWorkload.TABLENAME_PROPERTY;
import static site.ycsb.workloads.CoreWorkload.TABLENAME_PROPERTY_DEFAULT;

/**
 * HBase 2 client for YCSB framework.
 * <p>
 * Intended for use with HBase's shaded client.
 */
public class FdbClient extends site.ycsb.DB {
  private static final AtomicInteger THREAD_COUNT = new AtomicInteger(0);

  private FDBConfiguration config = FDBConfiguration.create();

  private boolean debug = false;

  private String tableName = "";

  public static final String NAMESPACE_PROPERTY = "namespace";

  /**
   * The default name of the database table to run queries against.
   */
  public static final String NAMESPACE_PROPERTY_DEFAULT = "userns";

  /**
   * A Cluster Connection instance that is shared by all running ycsb threads.
   * Needs to be initialized late so we pick up command-line configs if any.
   * To ensure one instance only in a multi-threaded context, guard access
   * with a 'lock' object.
   *
   * @See #CONNECTION_LOCK.
   */
  private static Connection connection = null;

  private Table currentTable = null;

  private String columnFamily = "";
  private byte[] columnFamilyBytes;

  /**
   * Whether or not a page filter should be used to limit scan length.
   */
  private boolean usePageFilter = true;

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    try {
      config.addResource(new File("src/test/resources/config/fdb-site.xml"));
    } catch (Exception e) {
      System.err.println("Keytab file is not readable or not found");
      throw new DBException(e);
    }

    // TODO authentication
//    if (config.getAuthentication() == KERBEROS) {
//      config.set("hadoop.security.authentication", "Kerberos");
//      UserGroupInformation.setConfiguration(config);
//    }
//
//    if ((getProperties().getProperty("principal") != null)
//        && (getProperties().getProperty("keytab") != null)) {
//      try {
//        UserGroupInformation.loginUserFromKeytab(getProperties().getProperty("principal"),
//            getProperties().getProperty("keytab"));
//      } catch (IOException e) {
//        System.err.println("Keytab file is not readable or not found");
//        throw new DBException(e);
//      }
//    }

    // TODO можно namespace брать из имени таблицы.
    String namespace = getProperties().getProperty(NAMESPACE_PROPERTY, NAMESPACE_PROPERTY_DEFAULT);
    String table = getProperties().getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);
    try {
      THREAD_COUNT.getAndIncrement();
      synchronized (THREAD_COUNT) {
        if (connection == null) {
          // Initialize if not set up already.
          connection = ConnectionFactory.createConnection(config);

          // Terminate right now if table does not exist, since the client
          // will not propagate this error upstream once the workload
          // starts.
          final TableName tName = TableName.valueOf(namespace, table);
          try (Admin admin = connection.getAdmin()) {
            if (!admin.isTableExists(tName)) {
              throw new DBException("Table " + tName + " does not exists");
            }
          }
        }
      }
    } catch (Exception e) {
      throw new DBException(e);
    }

    if ((getProperties().getProperty("debug") != null)
        && (getProperties().getProperty("debug").compareTo("true") == 0)) {
      debug = true;
    }

    usePageFilter = isBooleanParamSet("hbase.usepagefilter", usePageFilter);


    columnFamily = getProperties().getProperty("columnfamily");
    if (columnFamily == null) {
      System.err.println("Error, must specify a columnfamily for HBase table");
      throw new DBException("No columnfamily specified");
    }
    columnFamilyBytes = Bytes.toBytes(columnFamily);
  }

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    // Get the measurements instance as this is the only client that should
    // count clean up time like an update if client-side buffering is
    // enabled.
    Measurements measurements = Measurements.getMeasurements();
    try {
      long st = System.nanoTime();
      if (currentTable != null) {
        currentTable.close();
      }
      long en = System.nanoTime();
      final String type = "CLEANUP";
      measurements.measure(type, (int) ((en - st) / 1000));
      int threadCount = THREAD_COUNT.decrementAndGet();
      if (threadCount <= 0) {
        // Means we are done so ok to shut down the Connection.
        synchronized (THREAD_COUNT) {
          if (connection != null) {
            connection.close();
            connection = null;
          }
        }
      }
    } catch (Exception e) {
      throw new DBException(e);
    }
  }

  public void getTable(String namespace, String table) throws IOException {
    final TableName tName = TableName.valueOf(namespace, table);
    this.currentTable = connection.getTable(tName);
  }

  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error
   */
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    // if this is a "new" table, init HTable object. Else, use existing one
    if (!tableName.equals(table)) {
      currentTable = null;
      try {
        String namespace = getProperties().getProperty(NAMESPACE_PROPERTY, NAMESPACE_PROPERTY_DEFAULT);
        getTable(namespace, table);
        tableName = table;
      } catch (IOException e) {
        System.err.println("Error accessing HBase table: " + e);
        return Status.ERROR;
      }
    }

    Result r;
    try {
      if (debug) {
        System.out.println("Doing read from HBase columnfamily " + columnFamily);
        System.out.println("Doing read for key: " + key);
      }
      Get g = new Get(Bytes.toBytes(key));
      r = currentTable.get(g);
    } catch (IOException e) {
      if (debug) {
        System.err.println("Error doing get: " + e);
      }
      return Status.ERROR;
    } catch (ConcurrentModificationException e) {
      // do nothing for now...need to understand HBase concurrency model better
      return Status.ERROR;
    }

    try {
      while (r.advance()) {
        final Cell c = r.current();
        result.put(Bytes.toString(CellUtil.cloneQualifier(c)),
            new ByteArrayByteIterator(CellUtil.cloneValue(c)));
        if (debug) {
          System.out.println(
              "Result for field: " + Bytes.toString(CellUtil.cloneQualifier(c))
                  + " is: " + Bytes.toString(CellUtil.cloneValue(c)));
        }
      }
    } catch (IOException e) {
      if (debug) {
        System.err.println("Error getting result: " + e);
      }
      return Status.ERROR;
    }
    return Status.OK;
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   *
   * @param table       The name of the table
   * @param startkey    The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields      The list of fields to read, or null for all of them
   * @param result      A Vector of HashMaps, where each HashMap is a set field/value
   *                    pairs for one record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  // TODO scan does not exist
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    // if this is a "new" table, init Table object. Else, use existing one
    if (!tableName.equals(table)) {
      currentTable = null;
      try {
        String namespace = getProperties().getProperty(NAMESPACE_PROPERTY, NAMESPACE_PROPERTY_DEFAULT);
        getTable(namespace, table);
        tableName = table;
      } catch (IOException e) {
        System.err.println("Error accessing HBase table: " + e);
        return Status.ERROR;
      }
    }

    Get g = new Get(Bytes.toBytes(startkey));

    // get results
    try {
      Result r = currentTable.get(g);
      String key = new String(r.getRow());
      CellScanner scanner = r.cellScanner();
      StringBuilder sb = new StringBuilder();
      while (scanner.advance()) {
        Cell cell = r.current();
        sb.append("column family:")
            .append(new String(CellUtil.cloneFamily(cell)))
            .append(",")
            .append(new String(CellUtil.cloneQualifier(cell)))
            .append("=")
            .append(new String(CellUtil.cloneValue(cell)));
      }
      System.out.println("Got scan result for key: " + key);
    } catch (IOException e) {
      System.out.println("Error in getting/parsing scan result: " + e);
      return Status.ERROR;
    }

    return Status.OK;
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to write
   * @param values A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    // if this is a "new" table, init HTable object. Else, use existing one
    if (!tableName.equals(table)) {
      currentTable = null;
      try {
        String namespace = getProperties().getProperty(NAMESPACE_PROPERTY, NAMESPACE_PROPERTY_DEFAULT);
        getTable(namespace, table);
        tableName = table;
      } catch (IOException e) {
        System.err.println("Error accessing HBase table: " + e);
        return Status.ERROR;
      }
    }

    if (debug) {
      System.out.println("Setting up put for key: " + key);
    }
    Put p = new Put(Bytes.toBytes(key));
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      byte[] value = entry.getValue().toArray();
      if (debug) {
        System.out.println("Adding field/value " + entry.getKey() + "/"
            + Bytes.toStringBinary(value) + " to put request");
      }
      p.addColumn(columnFamilyBytes, Bytes.toBytes(entry.getKey()), value);
    }

    try {
      currentTable.put(p);
    } catch (IOException e) {
      if (debug) {
        System.err.println("Error doing put: " + e);
      }
      return Status.ERROR;
    } catch (ConcurrentModificationException e) {
      // do nothing for now...hope this is rare
      return Status.ERROR;
    }

    return Status.OK;
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {
    return update(table, key, values);
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key   The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  // TODO delete without parameters
  public Status delete(String table, String key) {
    // if this is a "new" table, init HTable object. Else, use existing one
    if (!tableName.equals(table)) {
      currentTable = null;
      try {
        String namespace = getProperties().getProperty(NAMESPACE_PROPERTY, NAMESPACE_PROPERTY_DEFAULT);
        getTable(namespace, table);
        tableName = table;
      } catch (IOException e) {
        System.err.println("Error accessing HBase table: " + e);
        return Status.ERROR;
      }
    }

    if (debug) {
      System.out.println("Doing delete for key: " + key);
    }

    final Delete d = new Delete();
    try {
      currentTable.delete(d);
    } catch (IOException e) {
      if (debug) {
        System.err.println("Error doing delete: " + e);
      }
      return Status.ERROR;
    }

    return Status.OK;
  }

  // Only non-private for testing.
  void setConfiguration(final FDBConfiguration newConfig) {
    this.config = newConfig;
  }

  private boolean isBooleanParamSet(String param, boolean defaultValue) {
    return Boolean.parseBoolean(getProperties().getProperty(param, Boolean.toString(defaultValue)));
  }

}

/*
 * For customized vim control set autoindent set si set shiftwidth=4
 */
