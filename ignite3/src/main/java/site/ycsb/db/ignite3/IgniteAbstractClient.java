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
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServer;
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
  protected static final String HOSTS_PROPERTY = "hosts";

  protected static final String PRIMARY_COLUMN_NAME = "ycsb_key";

  protected static final String DEFAULT_ZONE_NAME = "Z1";

  protected static final String DEFAULT_STORAGE_PROFILE_NAME = "default";

  protected static final String DEFAULT_COLUMNAR_PROFILE_NAME = "myColumnarStore";

  protected static final long TABLE_CREATION_TIMEOUT_SECONDS = 10L;

  private static final Logger LOG = LogManager.getLogger(IgniteAbstractClient.class);

  protected String cacheName;

  protected int fieldCount;

  protected String fieldPrefix;

  protected long recordsCount;

  protected long batchSize;

  protected final List<String> fields = new ArrayList<>();

  /**
   * Single Ignite client per process.
   */
  protected static Ignite ignite;

  protected IgniteClient igniteClient;

  protected static IgniteServer igniteServer;

  protected String hosts;

  protected KeyValueView<Tuple, Tuple> kvView;

  protected RecordView<Tuple> rView;

  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  private static volatile boolean initCompleted = false;

  private static volatile boolean externalIgnite = false;

  /**
   * Used to print more information into logs for debugging purposes.
   */
  protected static boolean debug = false;

  /**
   * Whether to shut down externally provided Ignite instance.
   */
  protected static boolean shutdownExternalIgnite = false;

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
   * Create table with columnar secondary storage profile and use it to fetch data.
   */
  protected static boolean useColumnar = false;

  /**
   * Used to choose storage engine (e.g., 'aipersist' or 'rocksdb').
   * @deprecated Removed in <a href="https://ggsystems.atlassian.net/browse/IGN-23905">IGN-23905</a>
   */
  @Deprecated
  protected static String dbEngine;

  /**
   * Used to choose storage profile (e.g., 'default', 'aimem', 'aipersist', 'rocksdb').
   */
  protected static String storageProfile;

  /**
   * Used to choose secondary storage profile (e.g., 'columnar').
   */
  protected static String secondaryStorageProfile;

  /**
   * Used to choose replication factor value.
   */
  protected static String replicas;

  /**
   * Used to choose partitions value.
   */
  protected static String partitions;

  /**
   * Set IgniteServer instance to work with.
   *
   * @param igniteSrv Ignite.
   */
  public static void setIgniteServer(IgniteServer igniteSrv) {
    igniteServer = igniteSrv;
    ignite = igniteServer.api();
    externalIgnite = true;
  }

  /** {@inheritDoc} */
  @Override
  public void init() throws DBException {
    INIT_COUNT.incrementAndGet();

    synchronized (IgniteAbstractClient.class) {
      if (initCompleted) {
        return;
      }

      initProperties(getProperties());

      initIgnite(useEmbeddedIgnite);

      createTestTable(ignite);

      initCompleted = true;
    }
  }

  /**
   * Init property values.
   *
   * @param properties Properties.
   */
  public void initProperties(Properties properties) throws DBException {
    try {
      debug = IgniteParam.DEBUG.getValue(properties);
      shutdownExternalIgnite = IgniteParam.SHUTDOWN_IGNITE.getValue(properties);
      useEmbeddedIgnite = IgniteParam.USE_EMBEDDED.getValue(properties);
      disableFsync = IgniteParam.DISABLE_FSYNC.getValue(properties);
      dbEngine = IgniteParam.DB_ENGINE.getValue(properties);
      storageProfile = IgniteParam.STORAGE_PROFILES.getValue(properties);
      useColumnar = IgniteParam.USE_COLUMNAR.getValue(properties);
      secondaryStorageProfile = IgniteParam.SECONDARY_STORAGE_PROFILE.getValue(properties);

      // backward compatibility of setting 'dbEngine' as storage engine name only.
      if (storageProfile.isEmpty() && !dbEngine.isEmpty()) {
        storageProfile = dbEngine;
      }

      replicas = IgniteParam.REPLICAS.getValue(properties);
      partitions = IgniteParam.PARTITIONS.getValue(properties);

      String workDirProperty = IgniteParam.WORK_DIR.getValue(properties);
      embeddedIgniteWorkDir = Paths.get(workDirProperty);

      cacheName = properties.getProperty(CoreWorkload.TABLENAME_PROPERTY,
          CoreWorkload.TABLENAME_PROPERTY_DEFAULT);
      fieldCount = Integer.parseInt(properties.getProperty(
          CoreWorkload.FIELD_COUNT_PROPERTY, CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT));
      fieldPrefix = properties.getProperty(CoreWorkload.FIELD_NAME_PREFIX,
          CoreWorkload.FIELD_NAME_PREFIX_DEFAULT);
      recordsCount = parseLongWithModifiers(properties.getProperty(Client.RECORD_COUNT_PROPERTY,
          Client.DEFAULT_RECORD_COUNT));
      batchSize = parseLongWithModifiers(properties.getProperty(Client.BATCH_SIZE_PROPERTY,
          Client.DEFAULT_BATCH_SIZE));

      for (int i = 0; i < fieldCount; i++) {
        fields.add(fieldPrefix + i);
      }

      hosts = properties.getProperty(HOSTS_PROPERTY);

      if (ignite == null && !useEmbeddedIgnite && hosts == null) {
        throw new DBException(String.format(
            "Required property \"%s\" is missing for Ignite Cluster",
            HOSTS_PROPERTY));
      }
    } catch (Exception e) {
      throw new DBException(e);
    }
  }

  /**
   * - Start embedded Ignite node (if needed).
   * - Get Ignite client (if needed).
   *
   * @param isEmbedded Whether to start embedded node.
   */
  private void initIgnite(boolean isEmbedded) throws DBException {
    //skip if 'ignite' was set with 'setIgniteServer'
    if (ignite == null) {
      if (isEmbedded) {
        igniteServer = startEmbeddedNode();
        ignite = igniteServer.api();
      } else {
        igniteClient = IgniteClient.builder().addresses(hosts.split(",")).build();
        ignite = igniteClient;
      }
    }
  }

  /**
   * Start embedded Ignite node.
   */
  private static IgniteServer startEmbeddedNode() throws DBException {
    IgniteServer embeddedIgnite;
    String clusterName = "myCluster";
    String nodeName = "defaultNode";

    try {
      String cfgResourceName = String.format("ignite-config%s%s.json",
          disableFsync ? "-nofsync" : "",
          storageProfile.isEmpty() ? "" : "-" + storageProfile);
      Path cfgPath = embeddedIgniteWorkDir.resolve(cfgResourceName);

      Files.createDirectories(embeddedIgniteWorkDir);
      try (InputStream cfgIs =
               IgniteAbstractClient.class.getClassLoader().getResourceAsStream(cfgResourceName)) {
        Files.copy(Objects.requireNonNull(cfgIs), cfgPath, StandardCopyOption.REPLACE_EXISTING);
      }

      LOG.info("Starting Ignite node {} in {} with config {}", nodeName, embeddedIgniteWorkDir, cfgPath);
      embeddedIgnite = IgniteServer.start(nodeName, cfgPath, embeddedIgniteWorkDir);

      InitParameters initParameters = InitParameters.builder()
          .metaStorageNodeNames(nodeName)
          .clusterName(clusterName)
          .build();

      embeddedIgnite.initCluster(initParameters);
    } catch (Exception e) {
      throw new DBException("Failed to start an embedded Ignite node", e);
    }

    return embeddedIgnite;
  }

  /**
   * Create test table.
   *
   * @param node0 Ignite node.
   */
  private void createTestTable(Ignite node0) throws DBException {
    try {

      String createZoneReq = createZoneSQL();

      String createTableReq = createTableSQL(createZoneReq);

      if (!createZoneReq.isEmpty()) {
        node0.sql().execute(null, createZoneReq).close();
      }

      node0.sql().execute(null, createTableReq).close();

      boolean cachePresent = waitForCondition(() -> ignite.tables().table(cacheName) != null,
          TABLE_CREATION_TIMEOUT_SECONDS * 1_000L);

      if (!cachePresent) {
        throw new DBException("Table wasn't created in " + TABLE_CREATION_TIMEOUT_SECONDS + " seconds.");
      }

      kvView = ignite.tables().table(cacheName).keyValueView();
      rView = ignite.tables().table(cacheName).recordView();
    } catch (Exception e) {
      throw new DBException(e);
    }
  }

  public String createTableSQL(String createZoneReq) {
    String fieldsSpecs = fields.stream()
        .map(e -> e + " VARCHAR")
        .collect(Collectors.joining(", "));

    String withZoneName = "";
    if (!createZoneReq.isEmpty()) {
      if (useColumnar) {
        withZoneName = String.format(
            " ZONE=\"%s\", STORAGE PROFILE '%s', SECONDARY STORAGE PROFILE '%s'",
            DEFAULT_ZONE_NAME,
            storageProfile,
            secondaryStorageProfile);
      } else {
        withZoneName = String.format(" ZONE \"%s\";", DEFAULT_ZONE_NAME);
      }
    }

    String createTableReq = String.format("CREATE TABLE IF NOT EXISTS %s(%s  VARCHAR PRIMARY KEY, %s) %s", cacheName,
        PRIMARY_COLUMN_NAME, fieldsSpecs, withZoneName);

    LOG.info("Create table request: {}", createTableReq);

    return createTableReq;
  }

  /**
   * Prepare the creation zone SQL line.
   */
  public String createZoneSQL() {
    if (storageProfile.isEmpty() && replicas.isEmpty() && partitions.isEmpty() && !useColumnar) {
      return "";
    }

    storageProfile = storageProfile.isEmpty() ?
        DEFAULT_STORAGE_PROFILE_NAME :
        storageProfile;
    secondaryStorageProfile = secondaryStorageProfile.isEmpty() ?
        DEFAULT_COLUMNAR_PROFILE_NAME :
        secondaryStorageProfile;

    String paramStorageProfile = String.format("STORAGE PROFILE '%s'", storageProfile);
    String paramSecondaryStorageProfile = String.format("SECONDARY STORAGE PROFILE '%s'", secondaryStorageProfile);
    String paramReplicas = replicas.isEmpty() ? "" : "replicas=" + replicas;
    String paramPartitions = partitions.isEmpty() ? "" : "partitions=" + partitions;

    String params = Stream.of(paramStorageProfile, paramSecondaryStorageProfile, paramReplicas, paramPartitions)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.joining(", "));

    String createZoneReq = String.format("CREATE ZONE IF NOT EXISTS %s WITH %s;", DEFAULT_ZONE_NAME, params);

    LOG.info("Create zone request: {}", createZoneReq);

    return createZoneReq;
  }

  /**
   * Get table entries amount.
   *
   * @param ignite0 Ignite node.
   * @param tableName Table name.
   */
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

  /** {@inheritDoc} */
  @Override
  public void cleanup() throws DBException {
    synchronized (IgniteAbstractClient.class) {
      int curInitCount = INIT_COUNT.decrementAndGet();

      if (curInitCount <= 0) {
        try {
          if (debug) {
            LOG.info("Records in table {}: {}", cacheName, entriesInTable(ignite, cacheName));
          }

          if (igniteClient != null) {
            igniteClient.close();
          }

          if (igniteServer != null && (!externalIgnite || shutdownExternalIgnite)) {
            igniteServer.shutdown();
          }

          ignite = null;
        } catch (Exception e) {
          throw new DBException(e);
        }
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }
}
