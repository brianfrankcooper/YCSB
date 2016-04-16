/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
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

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.Utils;

import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

//XXXX if we do replication, fix the consistency levels
/**
 * Cassandra 0.7 client for YCSB framework.
 */
public class CassandraClient7 extends DB {
  public static final ByteBuffer EMPTY_BYTE_BUFFER =
      ByteBuffer.wrap(new byte[0]);

  public static final String CONNECTION_RETRY_PROPERTY =
      "cassandra.connectionretries";
  public static final String CONNECTION_RETRY_PROPERTY_DEFAULT = "300";

  public static final String OPERATION_RETRY_PROPERTY =
      "cassandra.operationretries";
  public static final String OPERATION_RETRY_PROPERTY_DEFAULT = "300";

  public static final String USERNAME_PROPERTY = "cassandra.username";
  public static final String PASSWORD_PROPERTY = "cassandra.password";

  public static final String COLUMN_FAMILY_PROPERTY = "cassandra.columnfamily";
  public static final String COLUMN_FAMILY_PROPERTY_DEFAULT = "data";

  private int connectionRetries;
  private int operationRetries;
  private String columnFamily;

  private TTransport tr;
  private Cassandra.Client client;

  private boolean debug = false;

  private String tableName = "";
  private Exception errorexception = null;

  private List<Mutation> mutations = new ArrayList<Mutation>();
  private Map<String, List<Mutation>> mutationMap =
      new HashMap<String, List<Mutation>>();
  private Map<ByteBuffer, Map<String, List<Mutation>>> record =
      new HashMap<ByteBuffer, Map<String, List<Mutation>>>();

  private ColumnParent parent;

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  public void init() throws DBException {
    String hosts = getProperties().getProperty("hosts");
    if (hosts == null) {
      throw new DBException(
          "Required property \"hosts\" missing for CassandraClient");
    }

    columnFamily = getProperties().getProperty(COLUMN_FAMILY_PROPERTY,
        COLUMN_FAMILY_PROPERTY_DEFAULT);
    parent = new ColumnParent(columnFamily);

    connectionRetries =
        Integer.parseInt(getProperties().getProperty(CONNECTION_RETRY_PROPERTY,
            CONNECTION_RETRY_PROPERTY_DEFAULT));
    operationRetries =
        Integer.parseInt(getProperties().getProperty(OPERATION_RETRY_PROPERTY,
            OPERATION_RETRY_PROPERTY_DEFAULT));

    String username = getProperties().getProperty(USERNAME_PROPERTY);
    String password = getProperties().getProperty(PASSWORD_PROPERTY);

    debug = Boolean.parseBoolean(getProperties().getProperty("debug", "false"));

    String[] allhosts = hosts.split(",");
    String myhost = allhosts[Utils.random().nextInt(allhosts.length)];

    Exception connectexception = null;

    for (int retry = 0; retry < connectionRetries; retry++) {
      tr = new TFramedTransport(new TSocket(myhost, 9160));
      TProtocol proto = new TBinaryProtocol(tr);
      client = new Cassandra.Client(proto);
      try {
        tr.open();
        connectexception = null;
        break;
      } catch (Exception e) {
        connectexception = e;
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    if (connectexception != null) {
      System.err.println("Unable to connect to " + myhost + " after "
          + connectionRetries + " tries");
      System.out.println("Unable to connect to " + myhost + " after "
          + connectionRetries + " tries");
      throw new DBException(connectexception);
    }

    if (username != null && password != null) {
      Map<String, String> cred = new HashMap<String, String>();
      cred.put("username", username);
      cred.put("password", password);
      AuthenticationRequest req = new AuthenticationRequest(cred);
      try {
        client.login(req);
      } catch (Exception e) {
        throw new DBException(e);
      }
    }
  }

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  public void cleanup() throws DBException {
    tr.close();
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
  public Status read(String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {
    if (!tableName.equals(table)) {
      try {
        client.set_keyspace(table);
        tableName = table;
      } catch (Exception e) {
        e.printStackTrace();
        e.printStackTrace(System.out);
        return Status.ERROR;
      }
    }

    for (int i = 0; i < operationRetries; i++) {

      try {
        SlicePredicate predicate;
        if (fields == null) {
          SliceRange range = new SliceRange(EMPTY_BYTE_BUFFER,
              EMPTY_BYTE_BUFFER, false, 1000000);

          predicate = new SlicePredicate().setSlice_range(range);

        } else {
          ArrayList<ByteBuffer> fieldlist =
              new ArrayList<ByteBuffer>(fields.size());
          for (String s : fields) {
            fieldlist.add(ByteBuffer.wrap(s.getBytes("UTF-8")));
          }

          predicate = new SlicePredicate().setColumn_names(fieldlist);
        }

        List<ColumnOrSuperColumn> results =
            client.get_slice(ByteBuffer.wrap(key.getBytes("UTF-8")), parent,
                predicate, ConsistencyLevel.ONE);

        if (debug) {
          System.out.print("Reading key: " + key);
        }

        Column column;
        String name;
        ByteIterator value;
        for (ColumnOrSuperColumn oneresult : results) {

          column = oneresult.column;
          name = new String(column.name.array(),
              column.name.position() + column.name.arrayOffset(),
              column.name.remaining());
          value = new ByteArrayByteIterator(column.value.array(),
              column.value.position() + column.value.arrayOffset(),
              column.value.remaining());

          result.put(name, value);

          if (debug) {
            System.out.print("(" + name + "=" + value + ")");
          }
        }

        if (debug) {
          System.out.println();
        }

        return Status.OK;
      } catch (Exception e) {
        errorexception = e;
      }

      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    errorexception.printStackTrace();
    errorexception.printStackTrace(System.out);
    return Status.ERROR;

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
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    if (!tableName.equals(table)) {
      try {
        client.set_keyspace(table);
        tableName = table;
      } catch (Exception e) {
        e.printStackTrace();
        e.printStackTrace(System.out);
        return Status.ERROR;
      }
    }

    for (int i = 0; i < operationRetries; i++) {

      try {
        SlicePredicate predicate;
        if (fields == null) {
          SliceRange range = new SliceRange(EMPTY_BYTE_BUFFER,
              EMPTY_BYTE_BUFFER, false, 1000000);

          predicate = new SlicePredicate().setSlice_range(range);

        } else {
          ArrayList<ByteBuffer> fieldlist =
              new ArrayList<ByteBuffer>(fields.size());
          for (String s : fields) {
            fieldlist.add(ByteBuffer.wrap(s.getBytes("UTF-8")));
          }

          predicate = new SlicePredicate().setColumn_names(fieldlist);
        }

        KeyRange kr = new KeyRange().setStart_key(startkey.getBytes("UTF-8"))
            .setEnd_key(new byte[] {}).setCount(recordcount);

        List<KeySlice> results = client.get_range_slices(parent, predicate, kr,
            ConsistencyLevel.ONE);

        if (debug) {
          System.out.println("Scanning startkey: " + startkey);
        }

        HashMap<String, ByteIterator> tuple;
        for (KeySlice oneresult : results) {
          tuple = new HashMap<String, ByteIterator>();

          Column column;
          String name;
          ByteIterator value;
          for (ColumnOrSuperColumn onecol : oneresult.columns) {
            column = onecol.column;
            name = new String(column.name.array(),
                column.name.position() + column.name.arrayOffset(),
                column.name.remaining());
            value = new ByteArrayByteIterator(column.value.array(),
                column.value.position() + column.value.arrayOffset(),
                column.value.remaining());

            tuple.put(name, value);

            if (debug) {
              System.out.print("(" + name + "=" + value + ")");
            }
          }

          result.add(tuple);
          if (debug) {
            System.out.println();
          }
        }

        return Status.OK;
      } catch (Exception e) {
        errorexception = e;
      }
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    errorexception.printStackTrace();
    errorexception.printStackTrace(System.out);
    return Status.ERROR;
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   * 
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to write.
   * @param values
   *          A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error
   */
  public Status update(String table, String key,
      HashMap<String, ByteIterator> values) {
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
  public Status insert(String table, String key,
      HashMap<String, ByteIterator> values) {
    if (!tableName.equals(table)) {
      try {
        client.set_keyspace(table);
        tableName = table;
      } catch (Exception e) {
        e.printStackTrace();
        e.printStackTrace(System.out);
        return Status.ERROR;
      }
    }

    for (int i = 0; i < operationRetries; i++) {
      mutations.clear();
      mutationMap.clear();
      record.clear();

      if (debug) {
        System.out.println("Inserting key: " + key);
      }

      try {
        ByteBuffer wrappedKey = ByteBuffer.wrap(key.getBytes("UTF-8"));

        Column col;
        ColumnOrSuperColumn column;
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
          col = new Column();
          col.setName(ByteBuffer.wrap(entry.getKey().getBytes("UTF-8")));
          col.setValue(ByteBuffer.wrap(entry.getValue().toArray()));
          col.setTimestamp(System.currentTimeMillis());

          column = new ColumnOrSuperColumn();
          column.setColumn(col);

          mutations.add(new Mutation().setColumn_or_supercolumn(column));
        }

        mutationMap.put(columnFamily, mutations);
        record.put(wrappedKey, mutationMap);

        client.batch_mutate(record, ConsistencyLevel.ONE);

        return Status.OK;
      } catch (Exception e) {
        errorexception = e;
      }
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    errorexception.printStackTrace();
    errorexception.printStackTrace(System.out);
    return Status.ERROR;
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
  public Status delete(String table, String key) {
    if (!tableName.equals(table)) {
      try {
        client.set_keyspace(table);
        tableName = table;
      } catch (Exception e) {
        e.printStackTrace();
        e.printStackTrace(System.out);
        return Status.ERROR;
      }
    }

    for (int i = 0; i < operationRetries; i++) {
      try {
        client.remove(ByteBuffer.wrap(key.getBytes("UTF-8")),
            new ColumnPath(columnFamily), System.currentTimeMillis(),
            ConsistencyLevel.ONE);

        if (debug) {
          System.out.println("Delete key: " + key);
        }

        return Status.OK;
      } catch (Exception e) {
        errorexception = e;
      }
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    errorexception.printStackTrace();
    errorexception.printStackTrace(System.out);
    return Status.ERROR;
  }

  public static void main(String[] args) {
    CassandraClient7 cli = new CassandraClient7();

    Properties props = new Properties();

    props.setProperty("hosts", args[0]);
    cli.setProperties(props);

    try {
      cli.init();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(0);
    }

    HashMap<String, ByteIterator> vals = new HashMap<String, ByteIterator>();
    vals.put("age", new StringByteIterator("57"));
    vals.put("middlename", new StringByteIterator("bradley"));
    vals.put("favoritecolor", new StringByteIterator("blue"));
    Status res = cli.insert("usertable", "BrianFrankCooper", vals);
    System.out.println("Result of insert: " + res.getName());

    HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
    HashSet<String> fields = new HashSet<String>();
    fields.add("middlename");
    fields.add("age");
    fields.add("favoritecolor");
    res = cli.read("usertable", "BrianFrankCooper", null, result);
    System.out.println("Result of read: " + res.getName());
    for (Map.Entry<String, ByteIterator> entry : result.entrySet()) {
      System.out.println("[" + entry.getKey() + "]=[" + entry.getValue() + "]");
    }

    res = cli.delete("usertable", "BrianFrankCooper");
    System.out.println("Result of delete: " + res.getName());
  }
}
