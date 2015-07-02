package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;

public class AerospikeClient extends com.yahoo.ycsb.DB {
  private static final boolean DEBUG = false;

  private static final String DEFAULT_HOST = "localhost";
  private static final String DEFAULT_PORT = "3000";
  private static final String DEFAULT_TIMEOUT = "10000";
  private static final String DEFAULT_NAMESPACE = "ycsb";

  private static final int RESULT_OK = 0;
  private static final int RESULT_ERROR = -1;

  private static final int WRITE_OVERLOAD_DELAY = 5;
  private static final int WRITE_OVERLOAD_TRIES = 3;

  private String namespace = null;

  private com.aerospike.client.AerospikeClient client = null;

  private Policy readPolicy = new Policy();
  private WritePolicy insertPolicy = new WritePolicy();
  private WritePolicy updatePolicy = new WritePolicy();
  private WritePolicy deletePolicy = new WritePolicy();

  @Override
  public void init() throws DBException {
    insertPolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
    updatePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;

    Properties props = getProperties();

    namespace = props.getProperty("as.namespace", DEFAULT_NAMESPACE);

    String host = props.getProperty("as.host", DEFAULT_HOST);
    String user = props.getProperty("as.user");
    String password = props.getProperty("as.password");
    int port = Integer.parseInt(props.getProperty("as.port", DEFAULT_PORT));
    int timeout = Integer.parseInt(props.getProperty("as.timeout",
        DEFAULT_TIMEOUT));

    readPolicy.timeout = timeout;
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
  }

  @Override
  public void cleanup() throws DBException {
    client.close();
  }

  @Override
  public int read(String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {
    try {
      Record record;

      if (fields != null) {
        record = client.get(readPolicy, new Key(namespace, table, key),
            fields.toArray(new String[fields.size()]));
      } else {
        record = client.get(readPolicy, new Key(namespace, table, key));
      }

      if (record == null) {
        if (DEBUG) {
          System.err.println("Record key " + key + " not found (read)");
        }

        return RESULT_ERROR;
      }

      for (Map.Entry<String, Object> entry: record.bins.entrySet()) {
        result.put(entry.getKey(),
            new ByteArrayByteIterator((byte[])entry.getValue()));
      }

      return RESULT_OK;
    } catch (AerospikeException e) {
      System.err.println("Error while reading key " + key + ": " + e);
      return RESULT_ERROR;
    }
  }

  @Override
  public int scan(String table, String start, int count, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    System.err.println("Scan not implemented");
    return RESULT_ERROR;
  }

  private int write(String table, String key, WritePolicy writePolicy,
      HashMap<String, ByteIterator> values) {
    Bin[] bins = new Bin[values.size()];
    int index = 0;

    for (Map.Entry<String, ByteIterator> entry: values.entrySet()) {
      bins[index] = new Bin(entry.getKey(), entry.getValue().toArray());
      ++index;
    }

    int delay = WRITE_OVERLOAD_DELAY;
    Key keyObj = new Key(namespace, table, key);

    for (int tries = 0; tries < WRITE_OVERLOAD_TRIES; ++tries) {
      try {
        client.put(writePolicy, keyObj, bins);
        return RESULT_OK;
      } catch (AerospikeException e) {
        if (e.getResultCode() != ResultCode.DEVICE_OVERLOAD) {
          System.err.println("Error while writing key " + key + ": " + e);
          return RESULT_ERROR;
        }

        try {
          Thread.sleep(delay);
        } catch (InterruptedException e2) {
          if (DEBUG) {
            System.err.println("Interrupted: " + e2);
          }
        }

        delay *= 2;
      }
    }

    if (DEBUG) {
      System.err.println("Device overload");
    }

    return RESULT_ERROR;
  }

  @Override
  public int update(String table, String key,
      HashMap<String, ByteIterator> values) {
    return write(table, key, updatePolicy, values);
  }

  @Override
  public int insert(String table, String key,
      HashMap<String, ByteIterator> values) {
    return write(table, key, insertPolicy, values);
  }

  @Override
  public int delete(String table, String key) {
    try {
      if (!client.delete(deletePolicy, new Key(namespace, table, key))) {
        if (DEBUG) {
          System.err.println("Record key " + key + " not found (delete)");
        }

        return RESULT_ERROR;
      }

      return RESULT_OK;
    } catch (AerospikeException e) {
      System.err.println("Error while deleting key " + key + ": " + e);
      return RESULT_ERROR;
    }
  }
}
