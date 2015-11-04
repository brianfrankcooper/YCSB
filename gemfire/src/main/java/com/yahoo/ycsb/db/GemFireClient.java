/**
 * Copyright (c) 2013 - 2014 YCSB Contributors. All rights reserved.
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
import com.gemstone.gemfire.internal.admin.remote.DistributionLocatorId;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * VMware vFabric GemFire client for the YCSB benchmark.<br />
 * <p>By default acts as a GemFire client and tries to connect
 * to GemFire cache server running on localhost with default
 * cache server port. Hostname and port of a GemFire cacheServer
 * can be provided using <code>gemfire.serverport=port</code> and <code>
 * gemfire.serverhost=host</code> properties on YCSB command line.
 * A locator may also be used for discovering a cacheServer
 * by using the property <code>gemfire.locator=host[port]</code></p>
 * 
 * <p>To run this client in a peer-to-peer topology with other GemFire
 * nodes, use the property <code>gemfire.topology=p2p</code>. Running
 * in p2p mode will enable embedded caching in this client.</p>
 * 
 * <p>YCSB by default does its operations against "usertable". When running
 * as a client this is a <code>ClientRegionShortcut.PROXY</code> region,
 * when running in p2p mode it is a <code>RegionShortcut.PARTITION</code>
 * region. A cache.xml defining "usertable" region can be placed in the
 * working directory to override these region definitions.</p>
 * 
 * @author Swapnil Bawaskar (sbawaska at vmware)
 *
 */
public class GemFireClient extends DB {

  /** property name of the port where GemFire server is listening for connections */
  private static final String SERVERPORT_PROPERTY_NAME = "gemfire.serverport";

  /** property name of the host where GemFire server is running */
  private static final String SERVERHOST_PROPERTY_NAME = "gemfire.serverhost";

  /** default value of {@link #SERVERHOST_PROPERTY_NAME} */
  private static final String SERVERHOST_PROPERTY_DEFAULT = "localhost";

  /** property name to specify a GemFire locator. This property can be used in both
   * client server and p2p topology */
  private static final String LOCATOR_PROPERTY_NAME = "gemfire.locator";

  /** property name to specify GemFire topology */
  private static final String TOPOLOGY_PROPERTY_NAME = "gemfire.topology";

  /** value of {@value #TOPOLOGY_PROPERTY_NAME} when peer to peer topology should be used.
   *  (client-server topology is default) */
  private static final String TOPOLOGY_P2P_VALUE = "p2p";

  private GemFireCache cache;

  /**
   * true if ycsb client runs as a client to a
   * GemFire cache server
   */
  private boolean isClient;
  
  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    // hostName where GemFire cacheServer is running
    String serverHost = null;
    // port of GemFire cacheServer
    int serverPort = 0;
    String locatorStr = null;

    if (props != null && !props.isEmpty()) {
      String serverPortStr = props.getProperty(SERVERPORT_PROPERTY_NAME);
      if (serverPortStr != null) {
        serverPort = Integer.parseInt(serverPortStr);
      }
      serverHost = props.getProperty(SERVERHOST_PROPERTY_NAME, SERVERHOST_PROPERTY_DEFAULT);
      locatorStr = props.getProperty(LOCATOR_PROPERTY_NAME);
      
      String topology = props.getProperty(TOPOLOGY_PROPERTY_NAME);
      if (topology != null && topology.equals(TOPOLOGY_P2P_VALUE)) {
        CacheFactory cf = new CacheFactory();
        if (locatorStr != null) {
          cf.set("locators", locatorStr);
        }
        cache = cf.create();
        isClient = false;
        return;
      }
    }
    isClient = true;
    DistributionLocatorId locator = null;
    if (locatorStr != null) {
      locator = new DistributionLocatorId(locatorStr);
    }
    ClientCacheFactory ccf = new ClientCacheFactory();
    if (serverPort != 0) {
      ccf.addPoolServer(serverHost, serverPort);
    } else if (locator != null) {
      ccf.addPoolLocator(locator.getHost().getCanonicalHostName(), locator.getPort());
    }
    cache = ccf.create();
  }
  
  @Override
  public Status read(String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {
    Region<String, Map<String, byte[]>> r = getRegion(table);
    Map<String, byte[]> val = r.get(key);
    if (val != null) {
      if (fields == null) {
        for (String k : val.keySet()) {
          result.put(k, new ByteArrayByteIterator(val.get(k)));
        }
      } else {
        for (String field : fields) {
          result.put(field, new ByteArrayByteIterator(val.get(field)));
        }
      }
      return Status.OK;
    }
    return Status.ERROR;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    // GemFire does not support scan
    return Status.ERROR;
  }

  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    getRegion(table).put(key, convertToBytearrayMap(values));
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    getRegion(table).put(key, convertToBytearrayMap(values));
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    getRegion(table).destroy(key);
    return Status.OK;
  }

  private Map<String, byte[]> convertToBytearrayMap(Map<String,ByteIterator> values) {
    Map<String, byte[]> retVal = new HashMap<String, byte[]>();
    for (String key : values.keySet()) {
      retVal.put(key, values.get(key).toArray());
    }
    return retVal;
  }
  
  private Region<String, Map<String, byte[]>> getRegion(String table) {
    Region<String, Map<String, byte[]>> r = cache.getRegion(table);
    if (r == null) {
      try {
        if (isClient) {
          ClientRegionFactory<String, Map<String, byte[]>> crf = ((ClientCache) cache).createClientRegionFactory(ClientRegionShortcut.PROXY);
          r = crf.create(table);
        } else {
          RegionFactory<String, Map<String, byte[]>> rf = ((Cache)cache).createRegionFactory(RegionShortcut.PARTITION);
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
