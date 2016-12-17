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
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.api.commands.kv.UpdateValue;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.query.indexes.LongIntIndex;
import com.basho.riak.client.core.util.BinaryValue;
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
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;

import static com.yahoo.ycsb.db.riak.RiakUtils.createResultHashMap;
import static com.yahoo.ycsb.db.riak.RiakUtils.getKeyAsLong;
import static com.yahoo.ycsb.db.riak.RiakUtils.serializeTable;

/**
 * Riak KV 2.x.y client for YCSB framework.
 *
 */
public class RiakKVClient extends DB {
  private static final String HOST_PROPERTY = "riak.hosts";
  private static final String PORT_PROPERTY = "riak.port";
  private static final String BUCKET_TYPE_PROPERTY = "riak.bucket_type";
  private static final String R_VALUE_PROPERTY = "riak.r_val";
  private static final String W_VALUE_PROPERTY = "riak.w_val";
  private static final String READ_RETRY_COUNT_PROPERTY = "riak.read_retry_count";
  private static final String WAIT_TIME_BEFORE_RETRY_PROPERTY = "riak.wait_time_before_retry";
  private static final String TRANSACTION_TIME_LIMIT_PROPERTY = "riak.transaction_time_limit";
  private static final String STRONG_CONSISTENCY_PROPERTY = "riak.strong_consistency";
  private static final String STRONG_CONSISTENT_SCANS_BUCKET_TYPE_PROPERTY = "riak.strong_consistent_scans_bucket_type";
  private static final String DEBUG_PROPERTY = "riak.debug";

  private static final Status TIME_OUT = new Status("TIME_OUT", "Cluster didn't respond after maximum wait time.");

  private String[] hosts;
  private int port;
  private String bucketType;
  private String bucketType2i;
  private Quorum rvalue;
  private Quorum wvalue;
  private int readRetryCount;
  private int waitTimeBeforeRetry;
  private int transactionTimeLimit;
  private boolean strongConsistency;
  private String strongConsistentScansBucketType;
  private boolean performStrongConsistentScans;
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
    strongConsistentScansBucketType = propsPF.getProperty(STRONG_CONSISTENT_SCANS_BUCKET_TYPE_PROPERTY);
    debug = Boolean.parseBoolean(propsPF.getProperty(DEBUG_PROPERTY));
  }

  private void loadProperties() {
    // First, load the default properties...
    loadDefaultProperties();

    // ...then, check for some props set at command line!
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

    String strongConsistentScansBucketTypeString = props.getProperty(STRONG_CONSISTENT_SCANS_BUCKET_TYPE_PROPERTY);
    if (strongConsistentScansBucketTypeString != null) {
      strongConsistentScansBucketType = strongConsistentScansBucketTypeString;
    }

    String debugString = props.getProperty(DEBUG_PROPERTY);
    if (debugString != null) {
      debug = Boolean.parseBoolean(debugString);
    }
  }

  public void init() throws DBException {
    loadProperties();

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

    // If strong consistency is in use, we need to change the bucket-type where the 2i indexes will be stored.
    if (strongConsistency && !strongConsistentScansBucketType.isEmpty()) {
      // The 2i indexes have to be stored in the appositely created strongConsistentScansBucketType: this however has
      // to be done only if the user actually created it! So, if the latter doesn't exist, then the scan transactions
      // will not be performed at all.
      bucketType2i = strongConsistentScansBucketType;
      performStrongConsistentScans = true;
    } else {
      // If instead eventual consistency is in use, then the 2i indexes have to be stored in the bucket-type
      // indicated with the bucketType variable.
      bucketType2i = bucketType;
      performStrongConsistentScans = false;
    }

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

      if (strongConsistency) {
        System.err.println("Strong Consistent Scan Transactions " +  (performStrongConsistentScans ? "" : "NOT ") +
            "allowed.");
      }
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
    if (strongConsistency && !performStrongConsistentScans) {
      return Status.NOT_IMPLEMENTED;
    }

    // The strong consistent bucket-type is not capable of storing 2i indexes. So, we need to read them from the fake
    // one (which we use only to store indexes). This is why, when using such a consistency model, the bucketType2i
    // variable is set to FAKE_BUCKET_TYPE.
    IntIndexQuery iiq = new IntIndexQuery
        .Builder(new Namespace(bucketType2i, table), "key", getKeyAsLong(startkey), Long.MAX_VALUE)
        .withMaxResults(recordcount)
        .withPaginationSort(true)
        .build();

    Location location;
    RiakFuture<IntIndexQuery.Response, IntIndexQuery> future = riakClient.executeAsync(iiq);

    try {
      IntIndexQuery.Response response = future.get(transactionTimeLimit, TimeUnit.SECONDS);
      List<IntIndexQuery.Response.Entry> entries = response.getEntries();

      // If no entries were retrieved, then something bad happened...
      if (entries.size() == 0) {
        if (debug) {
          System.err.println("Unable to scan any record starting from key " + startkey + ", aborting transaction. " +
              "Reason: NOT FOUND");
        }

        return Status.NOT_FOUND;
      }

      for (IntIndexQuery.Response.Entry entry : entries) {
        // If strong consistency is in use, then the actual location of the object we want to read is obtained by
        // fetching the key from the one retrieved with the 2i indexes search operation.
        if (strongConsistency) {
          location = new Location(new Namespace(bucketType, table), entry.getRiakObjectLocation().getKeyAsString());
        } else {
          location = entry.getRiakObjectLocation();
        }

        FetchValue fv = new FetchValue.Builder(location)
            .withOption(FetchValue.Option.R, rvalue)
            .build();

        FetchValue.Response keyResponse = fetch(fv);

        if (keyResponse.isNotFound()) {
          if (debug) {
            System.err.println("Unable to scan all requested records starting from key " + startkey + ", aborting " +
                "transaction. Reason: NOT FOUND");
          }

          return Status.NOT_FOUND;
        }

        // Create the partial result to add to the result vector.
        HashMap<String, ByteIterator> partialResult = new HashMap<>();
        createResultHashMap(fields, keyResponse, partialResult);
        result.add(partialResult);
      }
    } catch (TimeoutException e) {
      if (debug) {
        System.err.println("Unable to scan all requested records starting from key " + startkey + ", aborting " +
            "transaction. Reason: TIME OUT");
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

    // Strong consistency doesn't support secondary indexing, but eventually consistent model does. So, we can mock a
    // 2i usage by creating a fake object stored in an eventually consistent bucket-type with the SAME KEY THAT THE
    // ACTUAL OBJECT HAS. This latter is obviously stored in the strong consistent bucket-type indicated with the
    // riak.bucket_type property.
    if (strongConsistency && performStrongConsistentScans) {
      // Create a fake object to store in the default bucket-type just to keep track of the 2i indices.
      Location fakeLocation = new Location(new Namespace(strongConsistentScansBucketType, table), key);

      // Obviously, we want the fake object to contain as less data as possible. We can't create a void object, so
      // we have to choose the minimum data size allowed: it is one byte.
      RiakObject fakeObject = new RiakObject();
      fakeObject.setValue(BinaryValue.create(new byte[]{0x00}));
      fakeObject.getIndexes().getIndex(LongIntIndex.named("key_int")).add(getKeyAsLong(key));

      StoreValue fakeStore = new StoreValue.Builder(fakeObject)
          .withLocation(fakeLocation)
          .build();

      // We don't mind whether the operation is finished or not, because waiting for it to complete would slow down the
      // client and make our solution too heavy to be seen as a valid compromise. This will obviously mean that under
      // heavy load conditions a scan operation could fail due to an unfinished "fakeStore".
      riakClient.executeAsync(fakeStore);
    } else if (!strongConsistency) {
      // The next operation is useless when using strong consistency model, so it's ok to perform it only when using
      // eventual consistency.
      object.getIndexes().getIndex(LongIntIndex.named("key_int")).add(getKeyAsLong(key));
    }

    // Store proper values into the object.
    object.setValue(BinaryValue.create(serializeTable(values)));

    StoreValue store = new StoreValue.Builder(object)
        .withOption(StoreValue.Option.W, wvalue)
        .withLocation(location)
        .build();

    RiakFuture<StoreValue.Response, Location> future = riakClient.executeAsync(store);

    try {
      future.get(transactionTimeLimit, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      if (debug) {
        System.err.println("Unable to insert key " + key + ". Reason: TIME OUT");
      }

      return TIME_OUT;
    } catch (Exception e) {
      if (debug) {
        System.err.println("Unable to insert key " + key + ". Reason: " + e.toString());
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
    // If eventual consistency model is in use, then an update operation is pratically equivalent to an insert one.
    if (!strongConsistency) {
      return insert(table, key, values);
    }

    Location location = new Location(new Namespace(bucketType, table), key);

    UpdateValue update = new UpdateValue.Builder(location)
        .withUpdate(new UpdateEntity(new RiakObject().setValue(BinaryValue.create(serializeTable(values)))))
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
   * problem by disallowing the siblings creation. Moreover, it disables strong consistency, because we don't have
   * the possibility to create a proper bucket-type to use to fake 2i indexes usage.
   *
   * @param bucket     The bucket name.
   * @throws Exception Thrown if something bad happens.
     */
  void setTestEnvironment(String bucket) throws Exception {
    bucketType = "default";
    bucketType2i = bucketType;
    strongConsistency = false;

    Namespace ns = new Namespace(bucketType, bucket);
    StoreBucketProperties newBucketProperties = new StoreBucketProperties.Builder(ns).withAllowMulti(false).build();

    riakClient.execute(newBucketProperties);
  }
}
