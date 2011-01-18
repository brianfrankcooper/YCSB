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

import com.yahoo.ycsb.*;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;
import java.util.Random;
import java.util.Properties;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.cassandra.thrift.*;

import org.apache.cassandra.utils.FBUtilities;

//XXXX if we do replication, fix the consistency levels
/**
 * Cassandra 0.7 client for YCSB framework
 */
public class CassandraClient7 extends DB
{
  static Random random = new Random();
  public static final int Ok = 0;
  public static final int Error = -1;

  public int ConnectionRetries;
  public int OperationRetries;
  public String column_family;

  public static final String CONNECTION_RETRY_PROPERTY = "cassandra.connectionretries";
  public static final String CONNECTION_RETRY_PROPERTY_DEFAULT = "300";

  public static final String OPERATION_RETRY_PROPERTY = "cassandra.operationretries";
  public static final String OPERATION_RETRY_PROPERTY_DEFAULT = "300";

  public static final String USERNAME_PROPERTY = "cassandra.username";
  public static final String PASSWORD_PROPERTY = "cassandra.password";

  public static final String COLUMN_FAMILY_PROPERTY = "cassandra.columnfamily";
  public static final String COLUMN_FAMILY_PROPERTY_DEFAULT = "data";

  TTransport tr;
  Cassandra.Client client;

  boolean _debug = false;

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  public void init() throws DBException
  {
    String hosts = getProperties().getProperty("hosts");
    if (hosts == null)
    {
      throw new DBException("Required property \"hosts\" missing for CassandraClient");
    }

    column_family = getProperties().getProperty(COLUMN_FAMILY_PROPERTY, COLUMN_FAMILY_PROPERTY_DEFAULT);

    ConnectionRetries = Integer.parseInt(getProperties().getProperty(CONNECTION_RETRY_PROPERTY,
        CONNECTION_RETRY_PROPERTY_DEFAULT));
    OperationRetries = Integer.parseInt(getProperties().getProperty(OPERATION_RETRY_PROPERTY,
        OPERATION_RETRY_PROPERTY_DEFAULT));

    String username = getProperties().getProperty(USERNAME_PROPERTY);
    String password = getProperties().getProperty(PASSWORD_PROPERTY);

    _debug = Boolean.parseBoolean(getProperties().getProperty("debug", "false"));

    String[] allhosts = hosts.split(",");
    String myhost = allhosts[random.nextInt(allhosts.length)];

    Exception connectexception = null;

    for (int retry = 0; retry < ConnectionRetries; retry++)
    {
      tr = new TFramedTransport(new TSocket(myhost, 9160));
      TProtocol proto = new TBinaryProtocol(tr);
      client = new Cassandra.Client(proto);
      try
      {
        tr.open();
        connectexception = null;
        break;
      } catch (Exception e)
      {
        connectexception = e;
      }
      try
      {
        Thread.sleep(1000);
      } catch (InterruptedException e)
      {
      }
    }
    if (connectexception != null)
    {
      System.err.println("Unable to connect to " + myhost + " after " + ConnectionRetries
          + " tries");
      System.out.println("Unable to connect to " + myhost + " after " + ConnectionRetries
          + " tries");
      throw new DBException(connectexception);
    }

    if (username != null && password != null) 
    {
        Map<String,String> cred = new HashMap<String,String>();
        cred.put("username", username);
        cred.put("password", password);
        AuthenticationRequest req = new AuthenticationRequest(cred);
        try 
        {
            client.login(req);
        }
        catch (Exception e)
        {
            throw new DBException(e);
        }
    }
  }

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  public void cleanup() throws DBException
  {
    tr.close();
  }

  private ByteBuffer tob(String str)
  {
    try
    {
      return ByteBuffer.wrap(str.getBytes("UTF-8"));
    }
    catch (UnsupportedEncodingException e)
    {
      throw new AssertionError(e);
    }
  }

  /** TODO: Next two methods copied from ByteBufferUtil in Cassandra trunk. */
  public static String tos(ByteBuffer buffer)
  {
    int offset = buffer.position();
    int length = buffer.remaining();
    Charset charset = Charset.defaultCharset();
    if (buffer.hasArray())
      return new String(buffer.array(), buffer.arrayOffset() + offset, length + buffer.arrayOffset(), charset);

    byte[] buff = getArray(buffer, offset, length);
    return new String(buff, charset);
  }

  public static byte[] getArray(ByteBuffer b, int start, int length)
  {
    if (b.hasArray())
      return Arrays.copyOfRange(b.array(), start + b.arrayOffset(), start + length + b.arrayOffset());

    byte[] bytes = new byte[length];

    for (int i = 0; i < length; i++)
    {
      bytes[i] = b.get(start++);
    }

    return bytes;
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
  public int read(String table, String key, Set<String> fields, HashMap<String, String> result)
  {
    Exception errorexception = null;
    try
    {
      client.set_keyspace(table);
    } catch (Exception e)
    {
      e.printStackTrace();
      e.printStackTrace(System.out);
      return Error;
    }

    for (int i = 0; i < OperationRetries; i++)
    {

      try
      {

        SlicePredicate predicate;
        if (fields == null)
        {

          SliceRange sliceRange = new SliceRange();
          sliceRange.setStart(new byte[0]);
          sliceRange.setFinish(new byte[0]);
          sliceRange.setCount(1000000);

          predicate = new SlicePredicate();
          predicate.setSlice_range(sliceRange);
        } else
        {
          ArrayList<ByteBuffer> fieldlist = new ArrayList<ByteBuffer>(fields.size());
          for (String s : fields)
          {
            fieldlist.add(tob(s));
          }

          predicate = new SlicePredicate();
          predicate.setColumn_names(fieldlist);
        }

        ColumnParent parent = new ColumnParent(column_family);
        List<ColumnOrSuperColumn> results = client.get_slice(tob(key), parent, predicate,
            ConsistencyLevel.ONE);

        if (_debug)
        {
          System.out.print("READ: ");
        }

        for (ColumnOrSuperColumn oneresult : results)
        {
          Column column = oneresult.column;
          result.put(tos(column.name), tos(column.value));

          if (_debug)
          {
            System.out.print("(" + tos(column.name) + "=" + tos(column.value) + ")");
          }
        }

        if (_debug)
        {
          System.out.println("");
        }

        return Ok;
      } catch (Exception e)
      {
        errorexception = e;
      }

      try
      {
        Thread.sleep(500);
      } catch (InterruptedException e)
      {
      }
    }
    errorexception.printStackTrace();
    errorexception.printStackTrace(System.out);
    return Error;

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
  public int scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, String>> result)
  {
    Exception errorexception = null;

    try
    {
      client.set_keyspace(table);
    } catch (Exception e)
    {
      e.printStackTrace();
      e.printStackTrace(System.out);
      return Error;
    }
    
    for (int i = 0; i < OperationRetries; i++)
    {

      try
      {
        SlicePredicate predicate;
        if (fields == null)
        {
          SliceRange sliceRange = new SliceRange();
          sliceRange.setStart(new byte[0]);
          sliceRange.setFinish(new byte[0]);
          sliceRange.setCount(1000000);
          predicate = new SlicePredicate();
          predicate.setSlice_range(sliceRange);
        } else
        {
          ArrayList<ByteBuffer> fieldlist = new ArrayList<ByteBuffer>(fields.size());
          for (String s : fields)
          {
            fieldlist.add(tob(s));
          }
          predicate = new SlicePredicate();
          predicate.setColumn_names(fieldlist);
        }
        ColumnParent parent = new ColumnParent(column_family);
        KeyRange kr = new KeyRange().setStart_key(tob(startkey)).setEnd_key(FBUtilities.EMPTY_BYTE_BUFFER).setCount(recordcount);

        List<KeySlice> results = client.get_range_slices(parent, predicate, kr, ConsistencyLevel.ONE);

        if (_debug)
        {
          System.out.println("SCAN:");
        }

        for (KeySlice oneresult : results)
        {
          HashMap<String, String> tuple = new HashMap<String, String>();

          for (ColumnOrSuperColumn onecol : oneresult.columns)
          {
            Column column = onecol.column;
            tuple.put(tos(column.name), tos(column.value));

            if (_debug)
            {
              System.out
                  .print("(" + tos(column.name) + "=" + tos(column.value) + ")");
            }
          }

          result.add(tuple);
          if (_debug)
          {
            System.out.println();
          }
        }

        return Ok;
      } catch (Exception e)
      {
        errorexception = e;
      }
      try
      {
        Thread.sleep(500);
      } catch (InterruptedException e)
      {
      }
    }
    errorexception.printStackTrace();
    errorexception.printStackTrace(System.out);
    return Error;
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
  public int update(String table, String key, HashMap<String, String> values)
  {
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
  public int insert(String table, String key, HashMap<String, String> values)
  {
    Exception errorexception = null;

    try
    {
      client.set_keyspace(table);
    } catch (Exception e)
    {
      e.printStackTrace();
      e.printStackTrace(System.out);
      return Error;
    }
    
    for (int i = 0; i < OperationRetries; i++)
    {
      // insert data
      long timestamp = System.currentTimeMillis();

      try
      {
        Map<ByteBuffer, Map<String, List<Mutation>>> batch_mutation = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
        ArrayList<Mutation> v = new ArrayList<Mutation>(values.size());
        Map<String, List<Mutation>> cfMutationMap = new HashMap<String, List<Mutation>>();
        cfMutationMap.put(column_family, v);
        batch_mutation.put(tob(key), cfMutationMap);

        for (String field : values.keySet())
        {
          String val = values.get(field);
          Column col = new Column(tob(field), tob(val), timestamp);

          ColumnOrSuperColumn c = new ColumnOrSuperColumn();
          c.setColumn(col);
          c.unsetSuper_column();
          Mutation m = new Mutation();
          m.setColumn_or_supercolumn(c);
          v.add(m);
        }

        client.batch_mutate(batch_mutation, ConsistencyLevel.ONE);

        if (_debug)
        {
          System.out.println("INSERT");
        }

        return Ok;
      } catch (Exception e)
      {
        errorexception = e;
      }
      try
      {
        Thread.sleep(500);
      } catch (InterruptedException e)
      {
      }
    }

    errorexception.printStackTrace();
    errorexception.printStackTrace(System.out);
    return Error;
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
  public int delete(String table, String key)
  {
    Exception errorexception = null;

    try
    {
      client.set_keyspace(table);
    } catch (Exception e)
    {
      e.printStackTrace();
      e.printStackTrace(System.out);
      return Error;
    }

    for (int i = 0; i < OperationRetries; i++)
    {
      try
      {
        client.remove(tob(key), new ColumnPath(column_family), System.currentTimeMillis(),
            ConsistencyLevel.ONE);

        if (_debug)
        {
          System.out.println("DELETE");
        }

        return Ok;
      } catch (Exception e)
      {
        errorexception = e;
      }
      try
      {
        Thread.sleep(500);
      } catch (InterruptedException e)
      {
      }
    }
    errorexception.printStackTrace();
    errorexception.printStackTrace(System.out);
    return Error;
  }

  public static void main(String[] args)
  {
    CassandraClient7 cli = new CassandraClient7();

    Properties props = new Properties();

    props.setProperty("hosts", args[0]);
    cli.setProperties(props);

    try
    {
      cli.init();
    } catch (Exception e)
    {
      e.printStackTrace();
      System.exit(0);
    }

    HashMap<String, String> vals = new HashMap<String, String>();
    vals.put("age", "57");
    vals.put("middlename", "bradley");
    vals.put("favoritecolor", "blue");
    int res = cli.insert("usertable", "BrianFrankCooper", vals);
    System.out.println("Result of insert: " + res);

    HashMap<String, String> result = new HashMap<String, String>();
    HashSet<String> fields = new HashSet<String>();
    fields.add("middlename");
    fields.add("age");
    fields.add("favoritecolor");
    res = cli.read("usertable", "BrianFrankCooper", null, result);
    System.out.println("Result of read: " + res);
    for (String s : result.keySet())
    {
      System.out.println("[" + s + "]=[" + result.get(s) + "]");
    }

    res = cli.delete("usertable", "BrianFrankCooper");
    System.out.println("Result of delete: " + res);
  }

  /*
   * public static void main(String[] args) throws TException,
   * InvalidRequestException, UnavailableException,
   * UnsupportedEncodingException, NotFoundException {
   * 
   * 
   * 
   * String key_user_id = "1";
   * 
   * 
   * 
   * 
   * client.insert("Keyspace1", key_user_id, new ColumnPath("Standard1", null,
   * "age".getBytes("UTF-8")), "24".getBytes("UTF-8"), timestamp,
   * ConsistencyLevel.ONE);
   * 
   * 
   * // read single column ColumnPath path = new ColumnPath("Standard1", null,
   * "name".getBytes("UTF-8"));
   * 
   * System.out.println(client.get("Keyspace1", key_user_id, path,
   * ConsistencyLevel.ONE));
   * 
   * 
   * // read entire row SlicePredicate predicate = new SlicePredicate(null, new
   * SliceRange(new byte[0], new byte[0], false, 10));
   * 
   * ColumnParent parent = new ColumnParent("Standard1", null);
   * 
   * List<ColumnOrSuperColumn> results = client.get_slice("Keyspace1",
   * key_user_id, parent, predicate, ConsistencyLevel.ONE);
   * 
   * for (ColumnOrSuperColumn result : results) {
   * 
   * Column column = result.column;
   * 
   * System.out.println(new String(column.name, "UTF-8") + " -> " + new
   * String(column.value, "UTF-8"));
   * 
   * }
   * 
   * 
   * 
   * 
   * }
   */
}
