/**
 * Copyright (c) 2015 YCSB contributors. All rights reserved.
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

package site.ycsb.db;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.*;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.util.*;
import java.util.stream.Collectors;

/**
 * YCSB binding for <a href="http://www.aerospike.com/">Areospike</a>.
 */
public class AerospikeBatchClient extends site.ycsb.DB {
  private static final String DEFAULT_HOST = "localhost";
  private static final String DEFAULT_PORT = "3000";
  private static final String DEFAULT_TIMEOUT = "10000";
  private static final String DEFAULT_NAMESPACE = "ycsb";

  private String namespace = null;

  private com.aerospike.client.AerospikeClient client = null;

  private Policy readPolicy = new Policy();
  private BatchPolicy batchPolicy = new BatchPolicy();
  private WritePolicy insertPolicy = new WritePolicy();
  private WritePolicy updatePolicy = new WritePolicy();
  private WritePolicy deletePolicy = new WritePolicy();

  private int allRecordsSize = 1000;
  private int readRecordsSize = 200;
  private List<Integer> allRecordsIndexes = new ArrayList<>(allRecordsSize);
  private List<Integer> readRecordsIndexes = new ArrayList<>(readRecordsSize);

  @Override
  public void init() throws DBException {
    insertPolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
    updatePolicy.recordExistsAction = RecordExistsAction.REPLACE_ONLY;

    Properties props = getProperties();

    namespace = props.getProperty("as.namespace", DEFAULT_NAMESPACE);

    String host = props.getProperty("as.host", DEFAULT_HOST);
    String user = props.getProperty("as.user");
    String password = props.getProperty("as.password");
    int port = Integer.parseInt(props.getProperty("as.port", DEFAULT_PORT));
    int timeout = Integer.parseInt(props.getProperty("as.timeout",
        DEFAULT_TIMEOUT));

    readPolicy.timeout = timeout;
    batchPolicy.timeout = timeout;
    insertPolicy.timeout = timeout;
    updatePolicy.timeout = timeout;
    deletePolicy.timeout = timeout;

    ClientPolicy clientPolicy = new ClientPolicy();

    if (user != null && password != null) {
      clientPolicy.user = user;
      clientPolicy.password = password;
    }

    try {
      client =
          new com.aerospike.client.AerospikeClient(clientPolicy, host, port);
    } catch (AerospikeException e) {
      throw new DBException(String.format("Error while creating Aerospike " +
          "client for %s:%d.", host, port), e);
    }

    for (int i = 0; i < allRecordsSize; i++) {
      allRecordsIndexes.add(i);
    }

    Set<Integer> readIndexSet = new HashSet<>(readRecordsSize);
    Random random = new Random();
    while (readIndexSet.size() < readRecordsSize){
      readIndexSet.add(random.nextInt(allRecordsSize));
    }
    readRecordsIndexes = readIndexSet.stream().collect(Collectors.toList());
  }

  @Override
  public void cleanup() throws DBException {
    client.close();
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    try {
      Key[] keys = readRecordsIndexes.stream().map(idx -> {
        String keyStr = key + "_" + idx.toString();
        Key asKey = new Key(namespace, table, keyStr);
        return asKey;
      }).collect(Collectors.toList()).toArray(new Key[1]);

      Record[] records;
      if (fields != null) {
        records = client.get(batchPolicy, keys,
            fields.toArray(new String[fields.size()]));
      } else {
        records = client.get(batchPolicy, keys);
      }

      // check the result of last record
      Record record = records[records.length - 1];
      if (record == null) {
        System.err.println("Record key " + key + " not found (read)");
        throw new NullPointerException("record is null");
      }
      for (Map.Entry<String, Object> entry: record.bins.entrySet()) {
        result.put(entry.getKey(),
            new ByteArrayByteIterator((byte[])entry.getValue()));
      }
      return Status.OK;
    } catch (NullPointerException e){
      return Status.ERROR;
    } catch (AerospikeException e) {
      System.err.println("Error while reading key " + key + ": " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String start, int count, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    System.err.println("Scan not implemented");
    return Status.ERROR;
  }

  private Status write(String table, String key, WritePolicy writePolicy,
      Map<String, ByteIterator> values) {
    System.out.println(key);
    Bin[] bins = new Bin[values.size()];
    int index = 0;

    for (Map.Entry<String, ByteIterator> entry: values.entrySet()) {
      bins[index] = new Bin(entry.getKey(), entry.getValue().toArray());
      ++index;
    }

    try{
      allRecordsIndexes.forEach(idx -> {
        String keyStr = key + "_" + idx;
        Key keyObj = new Key(namespace, table, keyStr);
        client.put(writePolicy, keyObj, bins);
      });
      return Status.OK;
    }catch (AerospikeException e){
      System.err.println("Error while writing key " + key + ": " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    return write(table, key, updatePolicy, values);
  }

  @Override
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {
    return write(table, key, insertPolicy, values);
  }

  @Override
  public Status delete(String table, String key) {
    try {
      if (!client.delete(deletePolicy, new Key(namespace, table, key))) {
        System.err.println("Record key " + key + " not found (delete)");
        return Status.ERROR;
      }

      return Status.OK;
    } catch (AerospikeException e) {
      System.err.println("Error while deleting key " + key + ": " + e);
      return Status.ERROR;
    }
  }
}
