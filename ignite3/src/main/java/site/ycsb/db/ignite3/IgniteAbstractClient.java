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

import static site.ycsb.Client.parseLongWithModifiers;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.InitParameters;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import site.ycsb.ByteIterator;
import site.ycsb.Client;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.workloads.CoreWorkload;

/**
 * Ignite abstract client.
 */
public abstract class IgniteAbstractClient extends DB {

  private static final Logger LOG = LogManager.getLogger(IgniteAbstractClient.class);

  protected static String cacheName;

  protected static int fieldCount;

  protected static String fieldPrefix;

  protected static long recordsCount;

  protected static long batchSize;

  protected static final List<String> FIELDS = new ArrayList<>();

  protected static final String HOSTS_PROPERTY = "hosts";

  protected static final String PRIMARY_COLUMN_NAME = "ycsb_key";

  protected static final String DEFAULT_ZONE_NAME = "Z1";

  protected static final long TABLE_CREATION_TIMEOUT_SECONDS = 10L;

  /**
   * Single Ignite thin client per process.
   */
  protected static Ignite node;

  protected static String hosts;

  protected static KeyValueView<Tuple, Tuple> kvView;

  protected RecordView<Tuple> rView;

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
   * Used to choose storage engine (e.g., 'aipersist' or 'rocksdb').
   * @deprecated Removed in <a href="https://ggsystems.atlassian.net/browse/IGN-23905">IGN-23905</a>
   */
  @Deprecated
  protected static String dbEngine;

  /**
   * Used to choose storage profiles separated by comma
   * (e.g., 'default', 'default_aimem', 'default_aipersist,default_rocksdb', etc.).
   */
  protected static String storageProfiles;

  /**
   * Used to choose replication factor value.
   */
  protected static String replicas;

  /**
   * Used to choose partitions value.
   */
  protected static String partitions;

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
        debug = IgniteParam.DEBUG.getValue(getProperties());
        useEmbeddedIgnite = IgniteParam.USE_EMBEDDED.getValue(getProperties());
        disableFsync = IgniteParam.DISABLE_FSYNC.getValue(getProperties());
        dbEngine = IgniteParam.DB_ENGINE.getValue(getProperties());
        storageProfiles = IgniteParam.STORAGE_PROFILES.getValue(getProperties());

        // backward compatibility of setting 'dbEngine' as storage engine name only.
        if (storageProfiles.isEmpty() && !dbEngine.isEmpty()) {
          if (!dbEngine.startsWith("default_")) {
            dbEngine = "default_" + dbEngine.toLowerCase();
          }

          storageProfiles = dbEngine;
        }

        replicas = IgniteParam.REPLICAS.getValue(getProperties());
        partitions = IgniteParam.PARTITIONS.getValue(getProperties());

        String workDirProperty = IgniteParam.WORK_DIR.getValue(getProperties());
        embeddedIgniteWorkDir = Paths.get(workDirProperty);

        cacheName = getProperties().getProperty(CoreWorkload.TABLENAME_PROPERTY,
            CoreWorkload.TABLENAME_PROPERTY_DEFAULT);
        fieldCount = Integer.parseInt(getProperties().getProperty(
            CoreWorkload.FIELD_COUNT_PROPERTY, CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT));
        fieldPrefix = getProperties().getProperty(CoreWorkload.FIELD_NAME_PREFIX,
            CoreWorkload.FIELD_NAME_PREFIX_DEFAULT);
        recordsCount = parseLongWithModifiers(getProperties().getProperty(Client.RECORD_COUNT_PROPERTY,
            Client.DEFAULT_RECORD_COUNT));
        batchSize = parseLongWithModifiers(getProperties().getProperty(Client.BATCH_SIZE_PROPERTY,
            Client.DEFAULT_BATCH_SIZE));

        for (int i = 0; i < fieldCount; i++) {
          FIELDS.add(fieldPrefix + i);
        }

        hosts = getProperties().getProperty(HOSTS_PROPERTY);

        if (!useEmbeddedIgnite && hosts == null) {
          throw new DBException(String.format(
              "Required property \"%s\" missing for Ignite Cluster",
              HOSTS_PROPERTY));
        }

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
    node = IgniteClient.builder().addresses(hosts.split(",")).build();
    createTestTable(node);
    kvView = node.tables().table(cacheName).keyValueView();
    rView = node.tables().table(cacheName).recordView();

    if (kvView == null) {
      throw new DBException("Failed to find cache: " + cacheName);
    }
  }

  private void initEmbeddedServerNode() throws DBException {
    node = startIgniteNode();
    createTestTable(node);
    kvView = node.tables().table(cacheName).keyValueView();
    rView = node.tables().table(cacheName).recordView();

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

      String createZoneReq = createZoneSQL();

      String withZoneName = createZoneReq.isEmpty() ?
          "" : String.format(" WITH PRIMARY_ZONE='%s';", DEFAULT_ZONE_NAME);

      String createTableReq = "CREATE TABLE IF NOT EXISTS " + cacheName + " ("
          + PRIMARY_COLUMN_NAME + " VARCHAR PRIMARY KEY, "
          + fieldsSpecs + ")" + withZoneName;

      LOG.info("Create table request: {}", createTableReq);

      if (!createZoneReq.isEmpty()) {
        node0.sql().execute(null, createZoneReq).close();
      }

      node0.sql().execute(null, createTableReq).close();

      boolean cachePresent = waitForCondition(() -> node.tables().table(cacheName) != null,
          TABLE_CREATION_TIMEOUT_SECONDS * 1_000L);

      if (!cachePresent) {
        throw new DBException("Table wasn't created in " + TABLE_CREATION_TIMEOUT_SECONDS + " seconds.");
      }
    } catch (Exception e) {
      throw new DBException(e);
    }
  }

  private String createZoneSQL() {
    if (storageProfiles.isEmpty() && replicas.isEmpty() && partitions.isEmpty()) {
      return "";
    }

    String paramStorageProfiles = String.format("STORAGE_PROFILES='%s'",
            storageProfiles.isEmpty() ? "default" : storageProfiles);
    String paramReplicas = replicas.isEmpty() ? "" : "replicas=" + replicas;
    String paramPartitions = partitions.isEmpty() ? "" : "partitions=" + partitions;
    String params = Stream.of(paramStorageProfiles, paramReplicas, paramPartitions)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.joining(", "));
    String reqWithParams = params.isEmpty() ? "" : " WITH " + params;

    String createZoneReq = "CREATE ZONE IF NOT EXISTS " + DEFAULT_ZONE_NAME + reqWithParams + ";";

    LOG.info("Create zone request: {}", createZoneReq);

    return createZoneReq;
  }

  private static long entriesInTable(Ignite ignite0, String tableName) throws DBException {
    long entries = 0L;

    try (ResultSet<SqlRow> res = ignite0.sql().execute(null, "SELECT COUNT(*) FROM " + tableName)) {
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
   * Try to get positive result for the given amount of time (but at least once, to mitigate some GC pauses).
   *
   * @param cond Condition to check.
   * @param timeout Timeout in milliseconds.
   * @return {@code True} if condition has happened within the timeout.
   */
  public static boolean waitForCondition(BooleanSupplier cond, long timeout) {
    return waitForCondition(cond, timeout, 50);
  }

  /**
   * Try to get positive result for the given amount of time (but at least once, to mitigate some GC pauses).
   *
   * @param cond Condition to check.
   * @param timeout Timeout in milliseconds.
   * @param interval Interval to test condition in milliseconds.
   * @return {@code True} if condition has happened within the timeout.
   */
  @SuppressWarnings("BusyWait")
  public static boolean waitForCondition(BooleanSupplier cond, long timeout, long interval) {
    long stop = System.currentTimeMillis() + timeout;

    do {
      if (cond.getAsBoolean()) {
        return true;
      }

      try {
        Thread.sleep(interval);
      } catch (InterruptedException e) {
        return false;
      }
    } while (System.currentTimeMillis() < stop);

    return false;
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
