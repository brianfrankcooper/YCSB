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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

/**
 *
 */
public class IgniteYCSBClient extends com.yahoo.ycsb.DB {
  private static final Logger LOG = LoggerFactory.getLogger(IgniteYCSBClient.class);
  private static Ignite ignite;
  private static IgniteCache<String, Map<String, String>> cache;


  static synchronized void createIgniteClient(String cacheName) {
    if (ignite == null) {
      System.out.println("IgniteYCSBClient.createIgniteClient");
      ignite = IgniteClient.startIgnite();
      cache = ignite.getOrCreateCache(cacheName);
    }
  }


  @Override
  public void init() throws DBException {
    System.out.println("IgniteYCSBClient.init");
    super.init();
    String tableName = getProperties().getProperty(CoreWorkload.TABLENAME_PROPERTY, CoreWorkload.TABLENAME_PROPERTY_DEFAULT);

    createIgniteClient(tableName);
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    LOG.debug("IgniteYCSBClient.read:{}",key);
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
    return null;
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
   *
   * @param table The name of the table
   * @param key The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return
   */

  @Override
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {
   LOG.debug("IgniteYCSBClient.insert:{}|{}", table, values);
   //todo: implement parameters driven batching and asynchronosity
    try {
      cache.putAsync(key, toCachedValue(values));
      return Status.OK;
    } catch (Exception e) {
      LOG.error("insert error", e);
      return Status.ERROR;
    }
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
