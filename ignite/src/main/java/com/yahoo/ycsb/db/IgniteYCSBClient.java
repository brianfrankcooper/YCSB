package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.workloads.CoreWorkload;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 *
 */
public class IgniteYCSBClient extends com.yahoo.ycsb.DB {

  public static final String INSERT_ASYNC = "insertAsync";
  public static final String IGNITE_IPS = "igniteIPs";
  public static final String BATCH_SIZE = "batchSize";

  private static final Logger LOG = LoggerFactory.getLogger(IgniteYCSBClient.class);
  private static Ignite ignite;
  private static IgniteCache<String, Map<String, String>> cache;

  private boolean asyncInsert = false;
  private int batchSize = 1;
  private int batchCount = 0;
  private Map<String, Map<String, String>> batchBuffer = new HashMap();
  private String tableName;


  static synchronized void createIgniteClient(String cacheName, String igniteIPs) {
    if (ignite == null) {
      LOG.debug("IgniteYCSBClient.createIgniteClient");
      ignite = IgniteClient.startIgnite(igniteIPs, true);
      cache = ignite.getOrCreateCache(cacheName);
    }
  }

  public String getTableName() {
    return tableName;
  }

  @Override
  public void init() throws DBException {
    super.init();
    Properties props = getProperties();
    tableName = props.getProperty(CoreWorkload.TABLENAME_PROPERTY, CoreWorkload.TABLENAME_PROPERTY_DEFAULT);
    String igniteIPs = props.getProperty(IGNITE_IPS, "127.0.0.1,127.0.0.1:47500..47509");
    asyncInsert = Boolean.valueOf(props.getProperty(INSERT_ASYNC, "false"));
    batchSize = Integer.parseInt(props.getProperty(BATCH_SIZE, "1"));
    createIgniteClient(tableName, igniteIPs);

    LOG.info(
        "" + this +  ":" +
        "\n\tBATCH_SIZE = " + batchSize +
        "\n\tINSERT_ASYNC = " +asyncInsert);

  }

  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    LOG.debug("IgniteYCSBClient.read:{}", key);
    Map<String, String> r = cache.get(key);
    if (r == null) {
      return Status.NOT_FOUND;
    } else {
      populateResult(result, fields, r);
      return Status.OK;
    }
  }

  private void populateResult(Map<String, ByteIterator> sink, Set<String> fields, Map<String, String> source) {
    if (fields == null) {
      for (Map.Entry<String, String> entry : source.entrySet()) {
        sink.put(entry.getKey(), new StringByteIterator(entry.getValue()));
      }
    } else {
      for (String fName : fields) {
        sink.put(fName, new StringByteIterator(source.get(fName)));
      }
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    LOG.warn("Ignite does not support scan semantics");
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    try {
      cache.put(key, toCachedValue(values));
      return Status.OK;
    } catch (Exception e) {
      LOG.error("update error", e);
      return Status.ERROR;
    }
  }


  /**
   * @param table  The name of the table
   * @param key    The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return
   */

  @Override
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {
    LOG.debug("IgniteYCSBClient.insert:{}|{}", table, values);
    Map<String, String> valToCache = toCachedValue(values);
    try {

      if (batchSize > 1) {
        batchBuffer.put(key, valToCache);
        if (++batchCount % batchSize == 0) {
          insertBuffer();
        }

      } else {
        if (asyncInsert) {
          cache.putAsync(key, valToCache);
        } else {
          cache.put(key, valToCache);
        }
      }


      return Status.OK;
    } catch (Exception e) {
      LOG.error("insert error", e);
      return Status.ERROR;
    }
  }

  @Override
  public void cleanup() throws DBException {
    //will submit all the remaining records in batch
    if (!batchBuffer.isEmpty()) {
      insertBuffer();
    }
    super.cleanup();

  }

  private void insertBuffer() {
    if (asyncInsert) {
      cache.putAllAsync(batchBuffer);
    } else {
      cache.putAll(batchBuffer);
    }
    batchBuffer.clear();
    batchCount = 0;
  }

  private Map<String, String> toCachedValue(Map<String, ByteIterator> values) {
    Map<String, String> res = new HashMap<>();
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      res.put(entry.getKey(), entry.getValue().toString());
    }
    return res;
  }

  @Override
  public Status delete(String table, String key) {
    try {
      cache.remove(key);
      return Status.OK;
    } catch (Exception e) {
      LOG.error("remove error", e);
      return Status.ERROR;
    }
  }
}
