/**
 * Copyright (c) 2013 - 2016 YCSB Contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.pdx.JSONFormatter;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;

/**
 * Apache Geode (incubating) client for the YCSB benchmark.<br />
 * <p>By default acts as a Geode client and tries to connect
 * to Geode cache server running on localhost with default
 * cache server port. Hostname and port of a Geode cacheServer
 * can be provided using <code>geode.serverport=port</code> and <code>
 * geode.serverhost=host</code> properties on YCSB command line.
 * A locator may also be used for discovering a cacheServer
 * by using the property <code>geode.locator=host[port]</code></p>
 * <p>
 * <p>To run this client in a peer-to-peer topology with other Geode
 * nodes, use the property <code>geode.topology=p2p</code>. Running
 * in p2p mode will enable embedded caching in this client.</p>
 * <p>
 * <p>YCSB by default does its operations against "usertable". When running
 * as a client this is a <code>ClientRegionShortcut.PROXY</code> region,
 * when running in p2p mode it is a <code>RegionShortcut.PARTITION</code>
 * region. A cache.xml defining "usertable" region can be placed in the
 * working directory to override these region definitions.</p>
 */
public class GeodeClient extends DB {
  /**
   * property name of the port where Geode server is listening for connections.
   */
  private static final String SERVERPORT_PROPERTY_NAME = "geode.serverport";

  /**
   * property name of the host where Geode server is running.
   */
  private static final String SERVERHOST_PROPERTY_NAME = "geode.serverhost";

  /**
   * default value of {@link #SERVERHOST_PROPERTY_NAME}.
   */
  private static final String SERVERHOST_PROPERTY_DEFAULT = "localhost";

  /**
   * property name to specify a Geode locator. This property can be used in both
   * client server and p2p topology
   */
  private static final String LOCATOR_PROPERTY_NAME = "geode.locator";

  /**
   * property name to specify Geode topology.
   */
  private static final String TOPOLOGY_PROPERTY_NAME = "geode.topology";

  /**
   * value of {@value #TOPOLOGY_PROPERTY_NAME} when peer to peer topology should be used.
   * (client-server topology is default)
   */
  private static final String TOPOLOGY_P2P_VALUE = "p2p";

  /**
   * Pattern to split up a locator string in the form host[port].
   */
  private static final Pattern LOCATOR_PATTERN = Pattern.compile("(.+)\\[(\\d+)\\]");;

  private GemFireCache cache;

  /**
   * true if ycsb client runs as a client to a Geode cache server.
   */
  private boolean isClient;

  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    // hostName where Geode cacheServer is running
    String serverHost = null;
    // port of Geode cacheServer
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

    ClientCacheFactory ccf = new ClientCacheFactory();
    ccf.setPdxReadSerialized(true);
    if (serverPort != 0) {
      ccf.addPoolServer(serverHost, serverPort);
    } else  {
      InetSocketAddress locatorAddress = getLocatorAddress(locatorStr);
      ccf.addPoolLocator(locatorAddress.getHostName(), locatorAddress.getPort());
    }
    cache = ccf.create();
  }

  static InetSocketAddress getLocatorAddress(String locatorStr) {
    Matcher matcher = LOCATOR_PATTERN.matcher(locatorStr);
    if(!matcher.matches()) {
      throw new IllegalStateException("Unable to parse locator: " + locatorStr);
    }
    return new InetSocketAddress(matcher.group(1), Integer.parseInt(matcher.group(2)));
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    Region<String, PdxInstance> r = getRegion(table);
    PdxInstance val = r.get(key);
    if (val != null) {
      if (fields == null) {
        for (String fieldName : val.getFieldNames()) {
          result.put(fieldName, new ByteArrayByteIterator((byte[]) val.getField(fieldName)));
        }
      } else {
        for (String field : fields) {
          result.put(field, new ByteArrayByteIterator((byte[]) val.getField(field)));
        }
      }
      return Status.OK;
    }
    return Status.ERROR;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    // Geode does not support scan
    return Status.ERROR;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    getRegion(table).put(key, convertToBytearrayMap(values));
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    getRegion(table).put(key, convertToBytearrayMap(values));
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    getRegion(table).destroy(key);
    return Status.OK;
  }

  private PdxInstance convertToBytearrayMap(Map<String, ByteIterator> values) {
    PdxInstanceFactory pdxInstanceFactory = cache.createPdxInstanceFactory(JSONFormatter.JSON_CLASSNAME);

    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      pdxInstanceFactory.writeByteArray(entry.getKey(), entry.getValue().toArray());
    }
    return pdxInstanceFactory.create();
  }

  private Region<String, PdxInstance> getRegion(String table) {
    Region<String, PdxInstance> r = cache.getRegion(table);
    if (r == null) {
      try {
        if (isClient) {
          ClientRegionFactory<String, PdxInstance> crf =
              ((ClientCache) cache).createClientRegionFactory(ClientRegionShortcut.PROXY);
          r = crf.create(table);
        } else {
          RegionFactory<String, PdxInstance> rf = ((Cache) cache).createRegionFactory(RegionShortcut.PARTITION);
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
