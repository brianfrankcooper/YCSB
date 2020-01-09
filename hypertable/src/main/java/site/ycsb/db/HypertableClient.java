/**
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

package site.ycsb.db;

import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;

import org.apache.thrift.TException;
import org.hypertable.thrift.SerializedCellsFlag;
import org.hypertable.thrift.SerializedCellsReader;
import org.hypertable.thrift.SerializedCellsWriter;
import org.hypertable.thrift.ThriftClient;
import org.hypertable.thriftgen.Cell;
import org.hypertable.thriftgen.ClientException;
import org.hypertable.thriftgen.Key;
import org.hypertable.thriftgen.KeyFlag;
import org.hypertable.thriftgen.RowInterval;
import org.hypertable.thriftgen.ScanSpec;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

/**
 * Hypertable client for YCSB framework.
 */
public class HypertableClient extends site.ycsb.DB {
  public static final String NAMESPACE = "/ycsb";
  public static final int THRIFTBROKER_PORT = 38080;
  public static final int BUFFER_SIZE = 4096;

  private boolean debug = false;

  private ThriftClient connection;
  private long ns;

  private String columnFamily = "";


  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    if ((getProperties().getProperty("debug") != null)
        && (getProperties().getProperty("debug").equals("true"))) {
      debug = true;
    }

    try {
      connection = ThriftClient.create("localhost", THRIFTBROKER_PORT);

      if (!connection.namespace_exists(NAMESPACE)) {
        connection.namespace_create(NAMESPACE);
      }
      ns = connection.open_namespace(NAMESPACE);
    } catch (ClientException e) {
      throw new DBException("Could not open namespace", e);
    } catch (TException e) {
      throw new DBException("Could not open namespace", e);
    }

    columnFamily = getProperties().getProperty("columnfamily");
    if (columnFamily == null) {
      System.err.println(
          "Error, must specify a " + "columnfamily for Hypertable table");
      throw new DBException("No columnfamily specified");
    }
  }

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    try {
      connection.namespace_close(ns);
    } catch (ClientException e) {
      throw new DBException("Could not close namespace", e);
    } catch (TException e) {
      throw new DBException("Could not close namespace", e);
    }
  }

  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to read.
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    // SELECT _column_family:field[i]
    // FROM table WHERE ROW=key MAX_VERSIONS 1;

    if (debug) {
      System.out
          .println("Doing read from Hypertable columnfamily " + columnFamily);
      System.out.println("Doing read for key: " + key);
    }

    try {
      if (null != fields) {
        Vector<HashMap<String, ByteIterator>> resMap =
            new Vector<HashMap<String, ByteIterator>>();
        if (!scan(table, key, 1, fields, resMap).equals(Status.OK)) {
          return Status.ERROR;
        }
        if (!resMap.isEmpty()) {
          result.putAll(resMap.firstElement());
        }
      } else {
        SerializedCellsReader reader = new SerializedCellsReader(null);
        reader.reset(connection.get_row_serialized(ns, table, key));
        while (reader.next()) {
          result.put(new String(reader.get_column_qualifier()),
              new ByteArrayByteIterator(reader.get_value()));
        }
      }
    } catch (ClientException e) {
      if (debug) {
        System.err.println("Error doing read: " + e.message);
      }
      return Status.ERROR;
    } catch (TException e) {
      if (debug) {
        System.err.println("Error doing read");
      }
      return Status.ERROR;
    }

    return Status.OK;
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   *
   * @param table
   *          The name of the table
   * @param startkey
   *          The record key of the first record to read.
   * @param recordcount
   *          The number of records to read
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A Vector of HashMaps, where each HashMap is a set field/value
   *          pairs for one record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    // SELECT _columnFamily:fields FROM table WHERE (ROW >= startkey)
    // LIMIT recordcount MAX_VERSIONS 1;

    ScanSpec spec = new ScanSpec();
    RowInterval elem = new RowInterval();
    elem.setStart_inclusive(true);
    elem.setStart_row(startkey);
    spec.addToRow_intervals(elem);
    if (null != fields) {
      for (String field : fields) {
        spec.addToColumns(columnFamily + ":" + field);
      }
    }
    spec.setVersions(1);
    spec.setRow_limit(recordcount);

    SerializedCellsReader reader = new SerializedCellsReader(null);

    try {
      long sc = connection.scanner_open(ns, table, spec);

      String lastRow = null;
      boolean eos = false;
      while (!eos) {
        reader.reset(connection.scanner_get_cells_serialized(sc));
        while (reader.next()) {
          String currentRow = new String(reader.get_row());
          if (!currentRow.equals(lastRow)) {
            result.add(new HashMap<String, ByteIterator>());
            lastRow = currentRow;
          }
          result.lastElement().put(new String(reader.get_column_qualifier()),
              new ByteArrayByteIterator(reader.get_value()));
        }
        eos = reader.eos();

        if (debug) {
          System.out
              .println("Number of rows retrieved so far: " + result.size());
        }
      }
      connection.scanner_close(sc);
    } catch (ClientException e) {
      if (debug) {
        System.err.println("Error doing scan: " + e.message);
      }
      return Status.ERROR;
    } catch (TException e) {
      if (debug) {
        System.err.println("Error doing scan");
      }
      return Status.ERROR;
    }

    return Status.OK;
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to write
   * @param values
   *          A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    return insert(table, key, values);
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to insert.
   * @param values
   *          A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status insert(String table, String key,
            Map<String, ByteIterator> values) {
    // INSERT INTO table VALUES
    // (key, _column_family:entry,getKey(), entry.getValue()), (...);

    if (debug) {
      System.out.println("Setting up put for key: " + key);
    }

    try {
      long mutator = connection.mutator_open(ns, table, 0, 0);
      SerializedCellsWriter writer =
          new SerializedCellsWriter(BUFFER_SIZE * values.size(), true);
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        writer.add(key, columnFamily, entry.getKey(),
            SerializedCellsFlag.AUTO_ASSIGN,
            ByteBuffer.wrap(entry.getValue().toArray()));
      }
      connection.mutator_set_cells_serialized(mutator, writer.buffer(), true);
      connection.mutator_close(mutator);
    } catch (ClientException e) {
      if (debug) {
        System.err.println("Error doing set: " + e.message);
      }
      return Status.ERROR;
    } catch (TException e) {
      if (debug) {
        System.err.println("Error doing set");
      }
      return Status.ERROR;
    }

    return Status.OK;
  }

  /**
   * Delete a record from the database.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status delete(String table, String key) {
    // DELETE * FROM table WHERE ROW=key;

    if (debug) {
      System.out.println("Doing delete for key: " + key);
    }

    Cell entry = new Cell();
    entry.key = new Key();
    entry.key.row = key;
    entry.key.flag = KeyFlag.DELETE_ROW;

    try {
      connection.set_cell(ns, table, entry);
    } catch (ClientException e) {
      if (debug) {
        System.err.println("Error doing delete: " + e.message);
      }
      return Status.ERROR;
    } catch (TException e) {
      if (debug) {
        System.err.println("Error doing delete");
      }
      return Status.ERROR;
    }

    return Status.OK;
  }
}
