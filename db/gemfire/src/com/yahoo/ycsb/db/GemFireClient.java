package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

public class GemFireClient extends DB {

  private static final int SUCCESS = 0;
  private static final int ERROR = -1;
  
  private GemFireCache cache;
  
  private int serverPort;
  
  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    if (props != null && !props.isEmpty()) {
      String serverPortStr = props.getProperty("gemfire.serverport");
      if (serverPortStr != null) {
        serverPort = Integer.parseInt(serverPortStr);
      }
      String topology = props.getProperty("gemfire.topology");
      if (topology != null) {
        if (topology.equals("embedded")) {
          cache = new CacheFactory().create();
        }
        return;
      }
    }
    cache = new ClientCacheFactory().create();
    
  }
  
  @Override
  public int read(String table, String key, Set<String> fields,
      HashMap<String, String> result) {
    Region<String, Map<String, String>> r = getRegion(table);
    Map<String, String> val = r.get(key);
    if (val != null) {
      if (fields == null) {
        result.putAll(val);
      } else {
        for (String field : fields) {
          result.put(field, val.get(field));
        }
      }
      return SUCCESS;
    }
    return ERROR;
  }

  @Override
  public int scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, String>> result) {
    // TODO 
    return SUCCESS;
  }

  @Override
  public int update(String table, String key, HashMap<String, String> values) {
    getRegion(table).put(key, values);
    return 0;
  }

  @Override
  public int insert(String table, String key, HashMap<String, String> values) {
    getRegion(table).put(key, values);
    return 0;
  }

  @Override
  public int delete(String table, String key) {
    getRegion(table).destroy(key);
    return 0;
  }

  private Region<String, Map<String, String>> getRegion(String table) {
    Region<String, Map<String, String>> r = cache.getRegion(table);
    if (r == null) {
      try {
        if (cache instanceof ClientCache) {
          ClientRegionFactory<String, Map<String, String>> crf = ((ClientCache) cache).createClientRegionFactory(ClientRegionShortcut.PROXY);
          r = crf.create(table);
        } else {
          RegionFactory<String, Map<String, String>> rf = ((Cache)cache).createRegionFactory(RegionShortcut.PARTITION);
          r = rf.create(table);
        }
      } catch (RegionExistsException e) {
        // another thread created the region
        r = cache.getRegion(table);
      }
    }
    return r;
  }

}
