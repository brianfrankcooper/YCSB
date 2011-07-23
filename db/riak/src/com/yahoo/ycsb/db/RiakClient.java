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

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;
import java.util.Random;
import java.util.Properties;
import java.nio.ByteBuffer;

import com.basho.riak.client.http.util.Constants;
import com.basho.riak.pbc.BucketProperties;
import com.basho.riak.pbc.RiakClient;
import com.basho.riak.pbc.RiakObject;
import com.google.protobuf.ByteString;

/**

   @author yourabi
**/
public class RiakClient extends DB {
  static Random random = new Random();
  public static final int Ok = 0;
  public static final int Error = -1;

  public int ConnectionRetries;
  public int OperationRetries;

  public static final String CONNECTION_RETRY_PROPERTY = "cassandra.connectionretries";
  public static final String CONNECTION_RETRY_PROPERTY_DEFAULT = "300";

  public static final String OPERATION_RETRY_PROPERTY = "cassandra.operationretries";
  public static final String OPERATION_RETRY_PROPERTY_DEFAULT = "300";

  public static final String USERNAME_PROPERTY = "cassandra.username";
  public static final String PASSWORD_PROPERTY = "cassandra.password";

  public static final String COLUMN_FAMILY_PROPERTY = "cassandra.columnfamily";
  public static final String COLUMN_FAMILY_PROPERTY_DEFAULT = "data";

  boolean _debug = false;
  IRiakClient riakClient; 

    
  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  public void init() throws DBException {
      
      String riakTransport = getProperties().getProperty("transport");
      if (riakTransport.equalsIngoreCase("http")) {
	  riakClient = RiakFactory.httpClient();
      } else if (riakTransport.equalsIgnoreCase("pb") || riakTransport == null) {
	  // RiakClient riak = new RiakClient("http://localhost:8098/riak");
	  riakClient =  RiakFactory.pbcClient();
      }


    column_family = getProperties().getProperty(COLUMN_FAMILY_PROPERTY, COLUMN_FAMILY_PROPERTY_DEFAULT);

    ConnectionRetries = Integer.parseInt(getProperties().getProperty(CONNECTION_RETRY_PROPERTY,
								     :qCONNECTION_RETRY_PROPERTY_DEFAULT));
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
  public int read(String table, String key, Set<String> fields, HashMap<String, String> result) {
      Bucket b = client.createBucket(bucketName)
	  .nVal(1)
	  .allowSiblings(true)
	  .execute();
      IRiakObject fetched = b.fetch("k").execute();
      b.store("k", "my new value").execute();
      b.delete("k").execute();


      /**
	 final Bucket carts = client.createBucket(bucketName).allowSiblings(true).execute();

final ShoppingCart cart = new ShoppingCart(userId);

cart.addItem("fixie");
cart.addItem("moleskine");

carts.store(cart).returnBody(false).retrier(DefaultRetrier.attempts(2)).execute();
      

bucket.store(o)
    .withConverter(converter)
    .withMutator(mutation)
    .withResolver(resolver)
    .r(r)
    .w(w)
    .dw(dw)
    .retrier(retrier)
    .returnBody(false)
.execute();



      // From quick start section
      // create a client
IRiakClient riakClient = RiakFactory.pbcClient(); //or RiakFactory.httpClient();

// create a new bucket
Bucket myBucket = riakClient.createBucket("myBucket").execute();

// add data to the bucket
myBucket.store("key1", "value1").execute();

//fetch it back
IRiakObject myData = myBucket.fetch("key1").execute();

// you can specify extra parameters to the store operation using the
// fluent builder style API
myData = myBucket.store("key1", "value2").returnBody(true).execute();

// delete
myBucket.delete("key1").rw(3).execute();
**/

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
          sliceRange.setStart(emptyByteBuffer);
          sliceRange.setFinish(emptyByteBuffer);;
          sliceRange.setCount(1000000);

          predicate = new SlicePredicate();
          predicate.setSlice_range(sliceRange);
        } else
        {
          ArrayList<ByteBuffer> fieldlist = new ArrayList<ByteBuffer>(fields.size());
          for (String s : fields)
          {
	    fieldlist.add(ByteBuffer.wrap(s.getBytes("UTF-8")));
          }

          predicate = new SlicePredicate();
          predicate.setColumn_names(fieldlist);
        }

        ColumnParent parent = new ColumnParent(column_family);
        List<ColumnOrSuperColumn> results = client.get_slice(ByteBuffer.wrap(key.getBytes("UTF-8")), parent, predicate,
            ConsistencyLevel.ONE);

        if (_debug)
        {
          System.out.print("READ: ");
        }

        for (ColumnOrSuperColumn oneresult : results)
        {

          Column column = oneresult.column;
	    
	  String name = new String(column.name.array(), column.name.position()+column.name.arrayOffset(), column.name.remaining());
	  String value = new String(column.value.array(), column.value.position()+column.value.arrayOffset(), column.value.remaining());

          result.put(name,value);

          if (_debug)
          {
            System.out.print("(" + name + "=" + value + ")");
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
          sliceRange.setStart(emptyByteBuffer);
          sliceRange.setFinish(emptyByteBuffer);
          sliceRange.setCount(1000000);
          predicate = new SlicePredicate();
          predicate.setSlice_range(sliceRange);
        } else
        {
          ArrayList<ByteBuffer> fieldlist = new ArrayList<ByteBuffer>(fields.size());
          for (String s : fields)
          {
	    fieldlist.add(ByteBuffer.wrap(s.getBytes("UTF-8")));
          }
          predicate = new SlicePredicate();
          predicate.setColumn_names(fieldlist);
        }
        ColumnParent parent = new ColumnParent(column_family);
        KeyRange kr = new KeyRange().setStart_key(startkey.getBytes("UTF-8")).setEnd_key(new byte[] {}).setCount(recordcount);

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
	    String name = new String(column.name.array(), column.name.position()+column.name.arrayOffset(), column.name.remaining());
	    String value = new String(column.value.array(), column.value.position()+column.value.arrayOffset(), column.value.remaining());
            
	    tuple.put(name, value);

            if (_debug)
            {
              System.out
                  .print("(" + name + "=" + value + ")");
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
        batch_mutation.put(ByteBuffer.wrap(key.getBytes("UTF-8")), cfMutationMap);

        for (String field : values.keySet())
        {
          String val = values.get(field);
          
          Column col = new Column();
          col.setName(ByteBuffer.wrap(field.getBytes("UTF-8")));
          col.setValue(ByteBuffer.wrap(val.getBytes("UTF-8")));
	  col.setTimestamp(timestamp);

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
      } catch (InvalidRequestException ire) {
	  ire.printStackTrace();
      } catch (Exception e) {
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
	client.remove(ByteBuffer.wrap(key.getBytes("UTF-8")), new ColumnPath(column_family), System.currentTimeMillis(),
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

  public static void main(String[] args) {
    RiakClient cli = new RiakClient();
    Properties props = new Properties();

    // props.setProperty("hosts", args[0]);
    // cli.setProperties(props);

    try {
      cli.init();
    } catch (Exception e) {
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
    
    for (String s : result.keySet()){
      System.out.println("[" + s + "]=[" + result.get(s) + "]");
    }

    res = cli.delete("usertable", "BrianFrankCooper");
    System.out.println("Result of delete: " + res);
  }
}
