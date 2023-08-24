/**
 * Copyright (c) 2013-2018 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 * <p>
 */

package site.ycsb.db.ignite3;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.InitParameters;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import site.ycsb.workloads.CoreWorkload;

/**
 * Ignite abstract client.
 */
public abstract class IgniteAbstractClient extends DB {

  private static final Logger LOG = LogManager.getLogger(IgniteAbstractClient.class);

  protected static String cacheName;

  protected static int fieldCount;

  protected static String fieldPrefix;

  protected static final List<String> FIELDS = new ArrayList<>();

  protected static final String HOSTS_PROPERTY = "hosts";

  protected static final String PORTS_PROPERTY = "ports";

  protected static final String PRIMARY_COLUMN_NAME = "yscb_key";

  /**
   * Single Ignite thin client per process.
   */
  protected static Ignite node;

  protected static String host;

  protected static String ports;

  protected static KeyValueView<Tuple, Tuple> kvView;

  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  /**
   * Used to print more information into logs for debugging purposes.
   */
  protected static boolean debug = false;

  /**
   * Start an embedded Ignite node instead of connecting to an external one.
   */
  protected static boolean useEmbeddedIgnite = false;

  protected static Path embeddedIgniteWorkDir;

  /**
   * Used to disable FSYNC by passing different config to an embedded Ignite node.
   */
  protected static boolean disableFsync = false;

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    INIT_COUNT.incrementAndGet();

    synchronized (IgniteAbstractClient.class) {
      if (node != null) {
        return;
      }

      try {
        debug = Boolean.parseBoolean(getProperties().getProperty("debug", "false"));
        useEmbeddedIgnite = Boolean.parseBoolean(getProperties().getProperty("useEmbedded", "false"));
        disableFsync = Boolean.parseBoolean(getProperties().getProperty("disableFsync", "false"));

        String workDirProperty = getProperties().getProperty("workDir",
            "../ignite3-ycsb-work/" + System.currentTimeMillis());
        embeddedIgniteWorkDir = Paths.get(workDirProperty);

        cacheName = getProperties().getProperty(CoreWorkload.TABLENAME_PROPERTY,
            CoreWorkload.TABLENAME_PROPERTY_DEFAULT);
        fieldCount = Integer.parseInt(getProperties().getProperty(
            CoreWorkload.FIELD_COUNT_PROPERTY, CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT));
        fieldPrefix = getProperties().getProperty(CoreWorkload.FIELD_NAME_PREFIX,
            CoreWorkload.FIELD_NAME_PREFIX_DEFAULT);

        for (int i = 0; i < fieldCount; i++) {
          FIELDS.add(fieldPrefix + i);
        }

        host = getProperties().getProperty(HOSTS_PROPERTY);
        if (!useEmbeddedIgnite && host == null) {
          throw new DBException(String.format(
              "Required property \"%s\" missing for Ignite Cluster",
              HOSTS_PROPERTY));
        }
        ports = getProperties().getProperty(PORTS_PROPERTY, "10800");

        if (useEmbeddedIgnite) {
          initEmbeddedServerNode();
        } else {
          initIgniteClientNode();
        }
      } catch (Exception e) {
        throw new DBException(e);
      }
    }
  }

  private void initIgniteClientNode() throws DBException {
    node = IgniteClient.builder().addresses(host + ":" + ports).build();
    createTestTable(node);
    kvView = node.tables().table(cacheName).keyValueView();
    if (kvView == null) {
      throw new DBException("Failed to find cache: " + cacheName);
    }
  }

  private void initEmbeddedServerNode() throws DBException {
    node = startIgniteNode();
    createTestTable(node);
    kvView = node.tables().table(cacheName).keyValueView();
    if (kvView == null) {
      throw new DBException("Failed to find cache: " + cacheName);
    }
  }

  private static Ignite startIgniteNode() throws DBException {
    Ignite ignite;
    String clusterName = "myCluster";
    String nodeName = "defaultNode";

    try {
      String cfgResourceName = disableFsync ? "ignite-config-nofsync.json" : "ignite-config.json";
      Path cfgPath = embeddedIgniteWorkDir.resolve(cfgResourceName);

      Files.createDirectories(embeddedIgniteWorkDir);
      try (InputStream cfgIs =
               IgniteAbstractClient.class.getClassLoader().getResourceAsStream(cfgResourceName)) {
        Files.copy(Objects.requireNonNull(cfgIs), cfgPath, StandardCopyOption.REPLACE_EXISTING);
      }

      LOG.info("Starting Ignite node {} in {} with config {}", nodeName, embeddedIgniteWorkDir, cfgPath);
      CompletableFuture<Ignite> fut = IgnitionManager.start(nodeName, cfgPath, embeddedIgniteWorkDir);

      InitParameters initParameters = InitParameters.builder()
          .destinationNodeName(nodeName)
          .metaStorageNodeNames(Collections.singletonList(nodeName))
          .clusterName(clusterName)
          .build();
      IgnitionManager.init(initParameters);

      ignite = fut.join();
    } catch (Exception e) {
      throw new DBException("Failed to start an embedded Ignite node", e);
    }

    return ignite;
  }

  private void createTestTable(Ignite node0) throws DBException {
    try {
      String fieldsSpecs = FIELDS.stream()
          .map(e -> e + " VARCHAR")
          .collect(Collectors.joining(", "));

      String request = "CREATE TABLE IF NOT EXISTS " + cacheName + " ("
          + PRIMARY_COLUMN_NAME + " VARCHAR PRIMARY KEY, "
          + fieldsSpecs + ")";

      LOG.info("Create table request: {}", request);

      try (Session ses = node0.sql().createSession()) {
        ses.execute(null, request).close();
      }
    } catch (Exception e) {
      throw new DBException(e);
    }
  }

  private static long entriesInTable(Ignite ignite0, String tableName) throws DBException {
    long entries = 0L;

    try (Session session = ignite0.sql().createSession();
        ResultSet<SqlRow> res = session.execute(null, "SELECT COUNT(*) FROM " + tableName)) {
      while (res.hasNext()) {
        SqlRow row = res.next();

        entries = row.longValue(0);
      }
    } catch (Exception e) {
      throw new DBException("Failed to get number of entries in table " + tableName, e);
    }

    return entries;
  }

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    synchronized (IgniteAbstractClient.class) {
      int curInitCount = INIT_COUNT.decrementAndGet();

      if (curInitCount <= 0) {
        try {
          if (debug) {
            LOG.info("Records in table {}: {}", cacheName, entriesInTable(node, cacheName));
          }

          node.close();
          node = null;
        } catch (Exception e) {
          throw new DBException(e);
        }
      }
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }
}
