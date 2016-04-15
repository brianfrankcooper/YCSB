/**
 * Copyright (c) 2016 YCSB contributors All rights reserved.
 * Copyright 2014 Basho Technologies, Inc.
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

package com.yahoo.ycsb.db.riak;

import com.basho.riak.client.api.commands.buckets.StoreBucketProperties;
import com.basho.riak.client.api.commands.kv.UpdateValue;
import com.basho.riak.client.core.RiakFuture;
import com.yahoo.ycsb.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.cap.Quorum;
import com.basho.riak.client.api.commands.indexes.IntIndexQuery;
import com.basho.riak.client.api.commands.kv.DeleteValue;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.api.commands.kv.StoreValue.Option;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.query.indexes.LongIntIndex;
import com.basho.riak.client.core.util.BinaryValue;

import static com.yahoo.ycsb.db.riak.RiakUtils.deserializeTable;
import static com.yahoo.ycsb.db.riak.RiakUtils.getKeyAsLong;
import static com.yahoo.ycsb.db.riak.RiakUtils.serializeTable;

/**
 * Riak KV 2.x.y client for YCSB framework.
 *
 */
public final class RiakKVClient extends DB {
  private static final String HOST_PROPERTY = "riak.hosts";
  private static final String PORT_PROPERTY = "riak.port";
  private static final String BUCKET_TYPE_PROPERTY = "riak.bucket_type";
  private static final String R_VALUE_PROPERTY = "riak.r_val";
  private static final String W_VALUE_PROPERTY = "riak.w_val";
  private static final String READ_RETRY_COUNT_PROPERTY = "riak.read_retry_count";
  private static final String WAIT_TIME_BEFORE_RETRY_PROPERTY = "riak.wait_time_before_retry";
  private static final String TRANSACTION_TIME_LIMIT_PROPERTY = "riak.transaction_time_limit";
  private static final String STRONG_CONSISTENCY_PROPERTY = "riak.strong_consistency";
  private static final String DEBUG_PROPERTY = "riak.debug";

  private static final Status TIME_OUT = new Status("TIME_OUT", "Cluster didn't respond after maximum wait time.");

  private String[] hosts;
  private int port;
  private String bucketType;
  private Quorum rvalue;
  private Quorum wvalue;
  private int readRetryCount;
  private int waitTimeBeforeRetry;
  private int transactionTimeLimit;
  private boolean strongConsistency;
  private boolean debug;

  private RiakClient riakClient;
  private RiakCluster riakCluster;

  private void loadDefaultProperties() {
    InputStream propFile = RiakKVClient.class.getClassLoader().getResourceAsStream("riak.properties");
    Properties propsPF = new Properties(System.getProperties());

    try {
      propsPF.load(propFile);
    } catch (IOException e) {
      e.printStackTrace();
    }

    hosts = propsPF.getProperty(HOST_PROPERTY).split(",");
    port = Integer.parseInt(propsPF.getProperty(PORT_PROPERTY));
    bucketType = propsPF.getProperty(BUCKET_TYPE_PROPERTY);
    rvalue = new Quorum(Integer.parseInt(propsPF.getProperty(R_VALUE_PROPERTY)));
    wvalue = new Quorum(Integer.parseInt(propsPF.getProperty(W_VALUE_PROPERTY)));
    readRetryCount = Integer.parseInt(propsPF.getProperty(READ_RETRY_COUNT_PROPERTY));
    waitTimeBeforeRetry = Integer.parseInt(propsPF.getProperty(WAIT_TIME_BEFORE_RETRY_PROPERTY));
    transactionTimeLimit = Integer.parseInt(propsPF.getProperty(TRANSACTION_TIME_LIMIT_PROPERTY));
    strongConsistency = Boolean.parseBoolean(propsPF.getProperty(STRONG_CONSISTENCY_PROPERTY));
    debug = Boolean.parseBoolean(propsPF.getProperty(DEBUG_PROPERTY));
  }

  private void loadProperties() {
    loadDefaultProperties();

    Properties props = getProperties();

    String portString = props.getProperty(PORT_PROPERTY);
    if (portString != null) {
      port = Integer.parseInt(portString);
    }

    String hostsString = props.getProperty(HOST_PROPERTY);
    if (hostsString != null) {
      hosts = hostsString.split(",");
    }

    String bucketTypeString = props.getProperty(BUCKET_TYPE_PROPERTY);
    if (bucketTypeString != null) {
      bucketType = bucketTypeString;
    }

    String rValueString = props.getProperty(R_VALUE_PROPERTY);
    if (rValueString != null) {
      rvalue = new Quorum(Integer.parseInt(rValueString));
    }

    String wValueString = props.getProperty(W_VALUE_PROPERTY);
    if (wValueString != null) {
      wvalue = new Quorum(Integer.parseInt(wValueString));
    }

    String readRetryCountString = props.getProperty(READ_RETRY_COUNT_PROPERTY);
    if (readRetryCountString != null) {
      readRetryCount = Integer.parseInt(readRetryCountString);
    }

    String waitTimeBeforeRetryString = props.getProperty(WAIT_TIME_BEFORE_RETRY_PROPERTY);
    if (waitTimeBeforeRetryString != null) {
      waitTimeBeforeRetry = Integer.parseInt(waitTimeBeforeRetryString);
    }

    String transactionTimeLimitString = props.getProperty(TRANSACTION_TIME_LIMIT_PROPERTY);
    if (transactionTimeLimitString != null) {
      transactionTimeLimit = Integer.parseInt(transactionTimeLimitString);
    }

    String strongConsistencyString = props.getProperty(STRONG_CONSISTENCY_PROPERTY);
    if (strongConsistencyString != null) {
      strongConsistency = Boolean.parseBoolean(strongConsistencyString);
    }

    String debugString = props.getProperty(DEBUG_PROPERTY);
    if (debugString != null) {
      debug = Boolean.parseBoolean(debugString);
    }
  }

  public void init() throws DBException {
    loadProperties();

    if (debug) {
      System.err.println("DEBUG ENABLED. Configuration parameters:");
      System.err.println("-----------------------------------------");
      System.err.println("Hosts: " + Arrays.toString(hosts));
      System.err.println("Port: " + port);
      System.err.println("Bucket Type: " + bucketType);
      System.err.println("R Val: " + rvalue.toString());
      System.err.println("W Val: " + wvalue.toString());
      System.err.println("Read Retry Count: " + readRetryCount);
      System.err.println("Wait Time Before Retry: " + waitTimeBeforeRetry + " ms");
      System.err.println("Transaction Time Limit: " + transactionTimeLimit + " s");
      System.err.println("Consistency model: " + (strongConsistency ? "Strong" : "Eventual"));
    }

    RiakNode.Builder builder = new RiakNode.Builder().withRemotePort(port);
    List<RiakNode> nodes = RiakNode.Builder.buildNodes(builder, Arrays.asList(hosts));
    riakCluster = new RiakCluster.Builder(nodes).build();

    try {
      riakCluster.start();
      riakClient = new RiakClient(riakCluster);
    } catch (Exception e) {
      System.err.println("Unable to properly start up the cluster. Reason: " + e.toString());
      throw new DBException(e);
    }
  }

  /**
   * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table  The name of the table (Riak bucket)
   * @param key    The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    Location location = new Location(new Namespace(bucketType, table), key);
    FetchValue fv = new FetchValue.Builder(location).withOption(FetchValue.Option.R, rvalue).build();
    FetchValue.Response response;

    try {
      response = fetch(fv);

      if (response.isNotFound()) {
        if (debug) {
          System.err.println("Unable to read key " + key + ". Reason: NOT FOUND");
        }

        return Status.NOT_FOUND;
      }
    } catch (TimeoutException e) {
      if (debug) {
        System.err.println("Unable to read key " + key + ". Reason: TIME OUT");
      }

      return TIME_OUT;
    } catch (Exception e) {
      if (debug) {
        System.err.println("Unable to read key " + key + ". Reason: " + e.toString());
      }

      return Status.ERROR;
    }

    // Create the result HashMap.
    createResultHashMap(fields, response, result);

    return Status.OK;
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in
   * a HashMap.
   * Note: The scan operation requires the use of secondary indexes (2i) and LevelDB.
   * IMPORTANT NOTE: the 2i queries DO NOT WORK in conjunction with strong consistency (ref: http://docs.basho
   * .com/riak/kv/2.1.4/developing/usage/secondary-indexes/)!
   *
   * @param table       The name of the table (Riak bucket)
   * @param startkey    The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields      The list of fields to read, or null for all of them
   * @param result      A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    // As of 2.1.4 Riak KV version, strong consistency does not support any suitable mean capable of searching
    // consecutive stored keys, as requested by a scan transaction. So, the latter WILL NOT BE PERFORMED AT ALL!
    // More info at http://docs.basho.com/riak/kv/2.1.4/developing/app-guide/strong-consistency/
    if (strongConsistency) {
      return Status.NOT_IMPLEMENTED;
    }

    Namespace ns = new Namespace(bucketType, table);

    IntIndexQuery iiq = new IntIndexQuery
        .Builder(ns, "key", getKeyAsLong(startkey), Long.MAX_VALUE)
        .withMaxResults(recordcount)
        .withPaginationSort(true)
        .build();

    RiakFuture<IntIndexQuery.Response, IntIndexQuery> future = riakClient.executeAsync(iiq);

    try {
      IntIndexQuery.Response response = future.get(transactionTimeLimit, TimeUnit.SECONDS);
      List<IntIndexQuery.Response.Entry> entries = response.getEntries();

      for (IntIndexQuery.Response.Entry entry : entries) {
        Location location = entry.getRiakObjectLocation();

        FetchValue fv = new FetchValue.Builder(location)
            .withOption(FetchValue.Option.R, rvalue)
            .build();

        FetchValue.Response keyResponse = fetch(fv);

        if (keyResponse.isNotFound()) {
          if (debug) {
            System.err.println("Unable to scan all records starting from key " + startkey + ", aborting transaction. " +
                "Reason: NOT FOUND");
          }

          return Status.NOT_FOUND;
        }

        HashMap<String, ByteIterator> partialResult = new HashMap<>();
        createResultHashMap(fields, keyResponse, partialResult);
        result.add(partialResult);
      }
    } catch (TimeoutException e) {
      if (debug) {
        System.err.println("Unable to scan all records starting from key " + startkey + ", aborting transaction. " +
            "Reason: TIME OUT");
      }

      return TIME_OUT;
    } catch (Exception e) {
      if (debug) {
        System.err.println("Unable to scan all records starting from key " + startkey + ", aborting transaction. " +
            "Reason: " + e.toString());
      }

      return Status.ERROR;
    }

    return Status.OK;
  }

  /**
   * Tries to perform a read and, whenever it fails, retries to do it. It actually does try as many time as indicated,
   * even if the function riakClient.execute(fv) throws an exception. This is needed for those situation in which the
   * cluster is unable to respond properly due to overload. Note however that if the cluster doesn't respond after
   * transactionTimeLimit, the transaction is discarded immediately.
   *
   * @param fv The value to fetch from the cluster.
   */
  private FetchValue.Response fetch(FetchValue fv) throws TimeoutException {
    FetchValue.Response response = null;

    for (int i = 0; i < readRetryCount; i++) {
      RiakFuture<FetchValue.Response, Location> future = riakClient.executeAsync(fv);

      try {
        response = future.get(transactionTimeLimit, TimeUnit.SECONDS);

        if (!response.isNotFound()) {
          break;
        }
      } catch (TimeoutException e) {
        // Let the callee decide how to handle this exception...
        throw new TimeoutException();
      } catch (Exception e) {
        // Sleep for a few ms before retrying...
        try {
          Thread.sleep(waitTimeBeforeRetry);
        } catch (InterruptedException e1) {
          e1.printStackTrace();
        }
      }
    }

    return response;
  }

  /**
   * Function that retrieves all the fields searched within a read or scan operation and puts them in the result
   * HashMap.
   *
   * @param fields        The list of fields to read, or null for all of them.
   * @param response      A Vector of HashMaps, where each HashMap is a set field/value pairs for one record.
   * @param resultHashMap The HashMap to return as result.
   */
  private void createResultHashMap(Set<String> fields, FetchValue.Response response, HashMap<String, ByteIterator>
      resultHashMap) {
    // If everything went fine, then a result must be given. Such an object is a hash table containing the (field,
    // value) pairs based on the requested fields. Note that in a read operation, ONLY ONE OBJECT IS RETRIEVED!
    // The following line retrieves the previously serialized table which was store with an insert transaction.
    byte[] responseFieldsAndValues = response.getValues().get(0).getValue().getValue();

    // Deserialize the stored response table.
    HashMap<String, ByteIterator> deserializedTable = new HashMap<>();
    deserializeTable(responseFieldsAndValues, deserializedTable);

    // If only specific fields are requested, then only these should be put in the result object!
    if (fields != null) {
      // Populate the HashMap to provide as result.
      for (Object field : fields.toArray()) {
        // Comparison between a requested field and the ones retrieved. If they're equal (i.e. the get() operation
        // DOES NOT return a null value), then  proceed to store the pair in the resultHashMap.
        ByteIterator value = deserializedTable.get(field);

        if (value != null) {
          resultHashMap.put((String) field, value);
        }
      }
    } else {
      // If, instead, no field is specified, then all the ones retrieved must be provided as result.
      for (String field : deserializedTable.keySet()) {
        resultHashMap.put(field, deserializedTable.get(field));
      }
    }
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the
   * record with the specified record key. Also creates a secondary index (2i) for each record consisting of the key
   * converted to long to be used for the scan operation.
   *
   * @param table  The name of the table (Riak bucket)
   * @param key    The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    Location location = new Location(new Namespace(bucketType, table), key);
    RiakObject object = new RiakObject();

    object.setValue(BinaryValue.create(serializeTable(values)));
    object.getIndexes().getIndex(LongIntIndex.named("key_int")).add(getKeyAsLong(key));

    StoreValue store = new StoreValue.Builder(object)
        .withLocation(location)
        .withOption(Option.W, wvalue)
        .build();

    RiakFuture<StoreValue.Response, Location> future = riakClient.executeAsync(store);

    try {
      future.get(transactionTimeLimit, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      if (debug) {
        System.err.println("Unable to " + (Thread.currentThread().getStackTrace()[2]
            .getMethodName().equals("update") ? "update" : "insert") + " key " + key + ". Reason: TIME OUT");
      }

      return TIME_OUT;
    } catch (Exception e) {
      if (debug) {
        System.err.println("Unable to " + (Thread.currentThread().getStackTrace()[2]
            .getMethodName().equals("update") ? "update" : "insert") + " key " + key + ". Reason: " + e.toString());
      }

      return Status.ERROR;
    }

    return Status.OK;
  }

  /**
   * Auxiliary class needed for object substitution within the update operation. It is a fundamental part of the
   * fetch-update (locally)-store cycle described by Basho to properly perform a strong-consistent update.
   */
  private static final class UpdateEntity extends UpdateValue.Update<RiakObject> {
    private final RiakObject object;

    private UpdateEntity(RiakObject object) {
      this.object = object;
    }

    //Simply returns the object.
    @Override
    public RiakObject apply(RiakObject original) {
      return object;
    }
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the
   * record with the specified record key, overwriting any existing values with the same field name.
   *
   * @param table  The name of the table (Riak bucket)
   * @param key    The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    if (!strongConsistency) {
      return insert(table, key, values);
    }

    Location location = new Location(new Namespace(bucketType, table), key);
    RiakObject object = new RiakObject();

    object.setValue(BinaryValue.create(serializeTable(values)));
    object.getIndexes().getIndex(LongIntIndex.named("key_int")).add(getKeyAsLong(key));

    UpdateValue update = new UpdateValue.Builder(location)
        .withUpdate(new UpdateEntity(object))
        .build();

    RiakFuture<UpdateValue.Response, Location> future = riakClient.executeAsync(update);

    try {
      // For some reason, the update transaction doesn't throw any exception when no cluster has been started, so one
      // needs to check whether it was done or not. When calling the wasUpdated() function with no nodes available, a
      // NullPointerException is thrown.
      // Moreover, such exception could be thrown when more threads are trying to update the same key or, more
      // generally, when the system is being queried by many clients (i.e. overloaded). This is a known limitation of
      // Riak KV's strong consistency implementation.
      future.get(transactionTimeLimit, TimeUnit.SECONDS).wasUpdated();
    } catch (TimeoutException e) {
      if (debug) {
        System.err.println("Unable to update key " + key + ". Reason: TIME OUT");
      }

      return TIME_OUT;
    } catch (Exception e) {
      if (debug) {
        System.err.println("Unable to update key " + key + ". Reason: " + e.toString());
      }

      return Status.ERROR;
    }

    return Status.OK;
  }


  /**
   * Delete a record from the database.
   *
   * @param table The name of the table (Riak bucket)
   * @param key   The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status delete(String table, String key) {
    Location location = new Location(new Namespace(bucketType, table), key);
    DeleteValue dv = new DeleteValue.Builder(location).build();

    RiakFuture<Void, Location> future = riakClient.executeAsync(dv);

    try {
      future.get(transactionTimeLimit, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      if (debug) {
        System.err.println("Unable to delete key " + key + ". Reason: TIME OUT");
      }

      return TIME_OUT;
    } catch (Exception e) {
      if (debug) {
        System.err.println("Unable to delete key " + key + ". Reason: " + e.toString());
      }

      return Status.ERROR;
    }

    return Status.OK;
  }

  public void cleanup() throws DBException {
    try {
      riakCluster.shutdown();
    } catch (Exception e) {
      System.err.println("Unable to properly shutdown the cluster. Reason: " + e.toString());
      throw new DBException(e);
    }
  }

  /**
   * Auxiliary function needed for testing. It configures the default bucket-type to take care of the consistency
   * problem by disallowing the siblings creation. Moreover, it disables strong consistency, as the scan transaction
   * test would otherwise fail.
   *
   * @param bucket     The bucket name.
   * @throws Exception Thrown if something bad happens.
     */
  void setTestEnvironment(String bucket) throws Exception {
    bucketType = "default";
    strongConsistency = false;

    Namespace ns = new Namespace(bucketType, bucket);
    StoreBucketProperties newBucketProperties = new StoreBucketProperties.Builder(ns).withAllowMulti(false).build();

    riakClient.execute(newBucketProperties);
  }
}
