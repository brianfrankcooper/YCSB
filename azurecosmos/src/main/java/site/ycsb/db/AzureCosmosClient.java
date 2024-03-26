/*
 * Copyright (c) 2018 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 */

package site.ycsb.db;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosDiagnosticsThresholds;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.DirectConnectionConfig;
import com.azure.cosmos.GatewayConnectionConfig;
import com.azure.cosmos.ThrottlingRetryOptions;
import com.azure.cosmos.implementation.apachecommons.lang.StringUtils;
import com.azure.cosmos.models.CosmosClientTelemetryConfig;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosPatchOperations;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.FeedResponse;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.SqlParameter;
import com.azure.cosmos.models.SqlQuerySpec;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Azure Cosmos DB Java V4 SDK client for YCSB.
 */

public class AzureCosmosClient extends DB {

  protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  // Default configuration values
  private static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.SESSION;
  private static final String DEFAULT_DATABASE_NAME = "ycsb";
  private static final boolean DEFAULT_USE_GATEWAY = false;
  private static final boolean DEFAULT_USE_UPSERT = false;
  private static final int DEFAULT_MAX_DEGREE_OF_PARALLELISM = -1;
  private static final int DEFAULT_MAX_BUFFERED_ITEM_COUNT = 0;
  private static final int DEFAULT_PREFERRED_PAGE_SIZE = -1;
  private static final int DEFAULT_DIAGNOSTICS_LATENCY_THRESHOLD_IN_MS = -1;
  private static final boolean DEFAULT_INCLUDE_EXCEPTION_STACK_IN_LOG = false;
  private static final String DEFAULT_USER_AGENT = "azurecosmos-ycsb";

  private static final Logger LOGGER = LoggerFactory.getLogger(AzureCosmosClient.class);
  private static final Marker CREATE_DIAGNOSTIC = MarkerFactory.getMarker("CREATE_DIAGNOSTIC");
  private static final Marker READ_DIAGNOSTIC = MarkerFactory.getMarker("READ_DIAGNOSTIC");
  private static final Marker PATCH_DIAGNOSTIC = MarkerFactory.getMarker("PATCH_DIAGNOSTIC");
  private static final Marker DELETE_DIAGNOSTIC = MarkerFactory.getMarker("DELETE_DIAGNOSTIC");
  private static final Marker QUERY_DIAGNOSTIC = MarkerFactory.getMarker("QUERY_DIAGNOSTIC");
  private static final Marker CREATE_EXCEPTION = MarkerFactory.getMarker("CREATE_EXCEPTION");
  private static final Marker READ_EXCEPTION = MarkerFactory.getMarker("READ_EXCEPTION");
  private static final Marker PATCH_EXCEPTION = MarkerFactory.getMarker("PATCH_EXCEPTION");
  private static final Marker DELETE_EXCEPTION = MarkerFactory.getMarker("DELETE_EXCEPTION");
  private static final Marker QUERY_EXCEPTION = MarkerFactory.getMarker("QUERY_EXCEPTION");


  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  private static CosmosClient client;
  private static CosmosDatabase database;
  private static String databaseName;
  private static boolean useUpsert;
  private static int maxDegreeOfParallelism;
  private static int maxBufferedItemCount;
  private static int preferredPageSize;
  private static int diagnosticsLatencyThresholdInMS;
  private static boolean includeExceptionStackInLog;
  private static Map<String, CosmosContainer> containerCache;
  private static String userAgent;

  private static Counter readSuccessCounter;
  private static Counter readFailureCounter;
  private static Timer readSuccessLatencyTimer;

  private static Counter scanSuccessCounter;
  private static Counter scanFailureCounter;
  private static Timer scanSuccessLatencyTimer;

  private static Counter writeSuccessCounter;
  private static Counter writeFailureCounter;
  private static Timer writeSuccessLatencyTimer;

  private static Counter updateSuccessCounter;
  private static Counter updateFailureCounter;
  private static Timer updateSuccessLatencyTimer;

  @Override
  public void init() throws DBException {
    INIT_COUNT.incrementAndGet();

    synchronized (INIT_COUNT) {
      if (client != null) {
        return;
      }
      try {
        initAzureCosmosClient();
      } catch (Exception e) {
        throw new DBException(e);
      }
    }
  }

  private void initAzureCosmosClient() throws DBException {

    // Connection properties
    String primaryKey = this.getStringProperty("azurecosmos.primaryKey", null);
    if (primaryKey == null || primaryKey.isEmpty()) {
      throw new DBException("Missing primary key required to connect to the database.");
    }

    String uri = this.getStringProperty("azurecosmos.uri", null);
    if (uri == null || uri.isEmpty()) {
      throw new DBException("Missing uri required to connect to the database.");
    }

    AzureCosmosClient.userAgent = this.getStringProperty("azurecosmos.userAgent", DEFAULT_USER_AGENT);

    AzureCosmosClient.useUpsert = this.getBooleanProperty("azurecosmos.useUpsert", DEFAULT_USE_UPSERT);

    AzureCosmosClient.databaseName = this.getStringProperty("azurecosmos.databaseName", DEFAULT_DATABASE_NAME);

    AzureCosmosClient.maxDegreeOfParallelism = this.getIntProperty("azurecosmos.maxDegreeOfParallelism",
        DEFAULT_MAX_DEGREE_OF_PARALLELISM);

    AzureCosmosClient.maxBufferedItemCount = this.getIntProperty("azurecosmos.maxBufferedItemCount",
        DEFAULT_MAX_BUFFERED_ITEM_COUNT);

    AzureCosmosClient.preferredPageSize = this.getIntProperty("azurecosmos.preferredPageSize",
        DEFAULT_PREFERRED_PAGE_SIZE);

    AzureCosmosClient.diagnosticsLatencyThresholdInMS = this.getIntProperty(
        "azurecosmos.diagnosticsLatencyThresholdInMS",
        DEFAULT_DIAGNOSTICS_LATENCY_THRESHOLD_IN_MS);

    AzureCosmosClient.includeExceptionStackInLog = this.getBooleanProperty("azurecosmos.includeExceptionStackInLog",
        DEFAULT_INCLUDE_EXCEPTION_STACK_IN_LOG);

    ConsistencyLevel consistencyLevel = ConsistencyLevel.valueOf(
        this.getStringProperty("azurecosmos.consistencyLevel", DEFAULT_CONSISTENCY_LEVEL.toString().toUpperCase()));
    boolean useGateway = this.getBooleanProperty("azurecosmos.useGateway", DEFAULT_USE_GATEWAY);

    ThrottlingRetryOptions retryOptions = new ThrottlingRetryOptions();
    int maxRetryAttemptsOnThrottledRequests = this.getIntProperty("azurecosmos.maxRetryAttemptsOnThrottledRequests",
        -1);
    if (maxRetryAttemptsOnThrottledRequests != -1) {
      retryOptions.setMaxRetryAttemptsOnThrottledRequests(maxRetryAttemptsOnThrottledRequests);
    }

    // Direct connection config options.
    DirectConnectionConfig directConnectionConfig = new DirectConnectionConfig();
    int directMaxConnectionsPerEndpoint = this.getIntProperty("azurecosmos.directMaxConnectionsPerEndpoint", -1);
    if (directMaxConnectionsPerEndpoint != -1) {
      directConnectionConfig.setMaxConnectionsPerEndpoint(directMaxConnectionsPerEndpoint);
    }

    int directIdleConnectionTimeoutInSeconds = this.getIntProperty("azurecosmos.directIdleConnectionTimeoutInSeconds",
        -1);
    if (directIdleConnectionTimeoutInSeconds != -1) {
      directConnectionConfig.setIdleConnectionTimeout(Duration.ofSeconds(directIdleConnectionTimeoutInSeconds));
    }

    // Gateway connection config options.
    GatewayConnectionConfig gatewayConnectionConfig = new GatewayConnectionConfig();

    int gatewayMaxConnectionPoolSize = this.getIntProperty("azurecosmos.gatewayMaxConnectionPoolSize", -1);
    if (gatewayMaxConnectionPoolSize != -1) {
      gatewayConnectionConfig.setMaxConnectionPoolSize(gatewayMaxConnectionPoolSize);
    }

    int gatewayIdleConnectionTimeoutInSeconds = this.getIntProperty("azurecosmos.gatewayIdleConnectionTimeoutInSeconds",
        -1);
    if (gatewayIdleConnectionTimeoutInSeconds != -1) {
      gatewayConnectionConfig.setIdleConnectionTimeout(Duration.ofSeconds(gatewayIdleConnectionTimeoutInSeconds));
    }

    String preferredRegions = this.getStringProperty("azurecosmos.preferredRegionList", null);
    List<String> preferredRegionList = null;
    if (StringUtils.isNotEmpty(preferredRegions)) {
      preferredRegions = preferredRegions.trim();
      preferredRegionList = new ArrayList<>(Arrays.asList(preferredRegions.split(",")));
    }

    int pointOperationLatencyThresholdInMS = this.getIntProperty("azurecosmos.pointOperationLatencyThresholdInMS",
        100);

    int nonPointOperationLatencyThresholdInMS = this.getIntProperty("azurecosmos.nonPointOperationLatencyThresholdInMS",
        500);

    int requestChargeThreshold = this.getIntProperty("azurecosmos.requestChargeThreshold", 100);

    try {
      LOGGER.info(
          "Creating Cosmos DB client {}, useGateway={}, consistencyLevel={},"
              + " maxRetryAttemptsOnThrottledRequests={}, maxRetryWaitTimeInSeconds={}"
              + " useUpsert={}, maxDegreeOfParallelism={}, maxBufferedItemCount={}, preferredPageSize={}",
          uri, useGateway, consistencyLevel.toString(), retryOptions.getMaxRetryAttemptsOnThrottledRequests(),
          retryOptions.getMaxRetryWaitTime().toMillis() / 1000, AzureCosmosClient.useUpsert,
          AzureCosmosClient.maxDegreeOfParallelism, AzureCosmosClient.maxBufferedItemCount,
          AzureCosmosClient.preferredPageSize);

      CosmosClientBuilder builder = new CosmosClientBuilder()
          .endpoint(uri)
          .key(primaryKey)
          .throttlingRetryOptions(retryOptions)
          .consistencyLevel(consistencyLevel)
          .userAgentSuffix(userAgent)
          .clientTelemetryConfig(new CosmosClientTelemetryConfig()
              .diagnosticsThresholds(
                  new CosmosDiagnosticsThresholds()
                      .setPointOperationLatencyThreshold(Duration.ofMillis(pointOperationLatencyThresholdInMS))
                      .setNonPointOperationLatencyThreshold(Duration.ofMillis(nonPointOperationLatencyThresholdInMS))
                      .setRequestChargeThreshold(requestChargeThreshold)));

      if (useGateway) {
        builder = builder.gatewayMode(gatewayConnectionConfig);
      } else {
        builder = builder.directMode(directConnectionConfig);
      }

      if (preferredRegionList != null && preferredRegionList.size() > 0) {
        builder.preferredRegions(preferredRegionList);
      }

      AzureCosmosClient.client = builder.buildClient();
      LOGGER.info("Azure Cosmos DB connection created to {}", uri);
    } catch (IllegalArgumentException e) {
      if (!AzureCosmosClient.includeExceptionStackInLog) {
        e = null;
      }
      throw new DBException("Illegal argument passed in. Check the format of your parameters.", e);
    }

    AzureCosmosClient.containerCache = new ConcurrentHashMap<>();

    // Verify the database exists
    try {
      AzureCosmosClient.database = AzureCosmosClient.client.getDatabase(databaseName);
      AzureCosmosClient.database.read();
    } catch (CosmosException e) {
      if (!AzureCosmosClient.includeExceptionStackInLog) {
        e = null;
      }
      throw new DBException(
          "Invalid database name (" + AzureCosmosClient.databaseName + ") or failed to read database.", e);
    }

    String appInsightConnectionString = this.getStringProperty("azurecosmos.appInsightConnectionString", null);
    if (appInsightConnectionString != null) {
      registerMeter();
    }
  }

  private String getStringProperty(String propertyName, String defaultValue) {
    return getProperties().getProperty(propertyName, defaultValue);
  }

  private boolean getBooleanProperty(String propertyName, boolean defaultValue) {
    String stringVal = getProperties().getProperty(propertyName, null);
    if (stringVal == null) {
      return defaultValue;
    }
    return Boolean.parseBoolean(stringVal);
  }

  private int getIntProperty(String propertyName, int defaultValue) {
    String stringVal = getProperties().getProperty(propertyName, null);
    if (stringVal == null) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(stringVal);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  private double getDoubleProperty(String propertyName, double defaultValue) {
    String stringVal = getProperties().getProperty(propertyName, null);
    if (stringVal == null) {
      return defaultValue;
    }
    try {
      return Double.parseDouble(stringVal);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    synchronized (INIT_COUNT) {
      if (INIT_COUNT.decrementAndGet() <= 0 && AzureCosmosClient.client != null) {
        try {
          AzureCosmosClient.client.close();
        } catch (Exception e) {
          if (!AzureCosmosClient.includeExceptionStackInLog) {
            e = null;
          }
          LOGGER.error("Could not close DocumentClient", e);
        } finally {
          AzureCosmosClient.client = null;
        }
      }
    }
  }

  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      long st = System.nanoTime();
      CosmosContainer container = AzureCosmosClient.containerCache.get(table);
      if (container == null) {
        container = AzureCosmosClient.database.getContainer(table);
        AzureCosmosClient.containerCache.put(table, container);
      }

      CosmosItemResponse<ObjectNode> response = container.readItem(key, new PartitionKey(key), ObjectNode.class);
      ObjectNode node = response.getItem();
      Map<String, String> stringResults = new HashMap<>(node.size());
      if (fields == null) {
        Iterator<Map.Entry<String, JsonNode>> iter = node.fields();
        while (iter.hasNext()) {
          Entry<String, JsonNode> pair = iter.next();
          stringResults.put(pair.getKey().toString(), pair.getValue().toString());
        }
        StringByteIterator.putAllAsByteIterators(result, stringResults);
      } else {
        Iterator<Map.Entry<String, JsonNode>> iter = node.fields();
        while (iter.hasNext()) {
          Entry<String, JsonNode> pair = iter.next();
          if (fields.contains(pair.getKey())) {
            stringResults.put(pair.getKey().toString(), pair.getValue().toString());
          }
        }
        StringByteIterator.putAllAsByteIterators(result, stringResults);
      }

      if (diagnosticsLatencyThresholdInMS > 0 &&
          response.getDiagnostics().getDuration().compareTo(Duration.ofMillis(diagnosticsLatencyThresholdInMS)) > 0) {
        LOGGER.warn(READ_DIAGNOSTIC, response.getDiagnostics().toString());
      }

      if (readSuccessLatencyTimer != null) {
        long en = System.nanoTime();
        long latency = (en - st) / 1000;
        readSuccessLatencyTimer.record(latency, TimeUnit.MICROSECONDS);
        readSuccessCounter.increment();
      }
      return Status.OK;
    } catch (CosmosException e) {
      int statusCode = e.getStatusCode();
      if (!AzureCosmosClient.includeExceptionStackInLog) {
        e = null;
      }
      LOGGER.error(READ_EXCEPTION, "Failed to read key {} in collection {} in database {} statusCode {}", key, table,
          AzureCosmosClient.databaseName, statusCode, e);
      if (readFailureCounter != null) {
        readFailureCounter.increment();
      }
      return Status.NOT_FOUND;
    }
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   *
   * @param table       The name of the table
   * @param startkey    The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields      The list of fields to read, or null for all of them
   * @param result      A Vector of HashMaps, where each HashMap is a set
   *                    field/value pairs for one record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    try {
      long st = System.nanoTime();
      CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();
      queryOptions.setMaxDegreeOfParallelism(AzureCosmosClient.maxDegreeOfParallelism);
      queryOptions.setMaxBufferedItemCount(AzureCosmosClient.maxBufferedItemCount);

      CosmosContainer container = AzureCosmosClient.containerCache.get(table);
      if (container == null) {
        container = AzureCosmosClient.database.getContainer(table);
        AzureCosmosClient.containerCache.put(table, container);
      }

      List<SqlParameter> paramList = new ArrayList<>();
      paramList.add(new SqlParameter("@startkey", startkey));

      SqlQuerySpec querySpec = new SqlQuerySpec(
          this.createSelectTop(fields, recordcount) + " FROM root r WHERE r.id >= @startkey", paramList);
      CosmosPagedIterable<ObjectNode> pagedIterable = container.queryItems(querySpec, queryOptions, ObjectNode.class);
      Iterator<FeedResponse<ObjectNode>> pageIterator = pagedIterable
          .iterableByPage(AzureCosmosClient.preferredPageSize).iterator();
      while (pageIterator.hasNext()) {
        FeedResponse<ObjectNode> feedResponse = pageIterator.next();
        List<ObjectNode> pageDocs = feedResponse.getResults();
        for (ObjectNode doc : pageDocs) {
          Map<String, String> stringResults = new HashMap<>(doc.size());
          Iterator<Map.Entry<String, JsonNode>> nodeIterator = doc.fields();
          while (nodeIterator.hasNext()) {
            Entry<String, JsonNode> pair = nodeIterator.next();
            stringResults.put(pair.getKey().toString(), pair.getValue().toString());
          }
          HashMap<String, ByteIterator> byteResults = new HashMap<>(doc.size());
          StringByteIterator.putAllAsByteIterators(byteResults, stringResults);
          result.add(byteResults);
        }
      }

      if (scanSuccessLatencyTimer != null) {
        long en = System.nanoTime();
        long latency = (en - st) / 1000;
        scanSuccessLatencyTimer.record(latency, TimeUnit.MICROSECONDS);
        scanSuccessCounter.increment();
      }
      return Status.OK;
    } catch (CosmosException e) {
      int statusCode = e.getStatusCode();
      if (!AzureCosmosClient.includeExceptionStackInLog) {
        e = null;
      }
      LOGGER.error(QUERY_EXCEPTION, "Failed to query key {} from collection {} in database {} statusCode {}",
          startkey, table, AzureCosmosClient.databaseName, statusCode, e);
    }
    if (scanFailureCounter != null) {
      scanFailureCounter.increment();
    }
    return Status.ERROR;
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record key,
   * overwriting any existing values with the same field name.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    try {
      long st = System.nanoTime();
      CosmosContainer container = AzureCosmosClient.containerCache.get(table);
      if (container == null) {
        container = AzureCosmosClient.database.getContainer(table);
        AzureCosmosClient.containerCache.put(table, container);
      }

      CosmosPatchOperations cosmosPatchOperations = CosmosPatchOperations.create();
      for (Entry<String, ByteIterator> pair : values.entrySet()) {
        cosmosPatchOperations.replace("/" + pair.getKey(), pair.getValue().toString());
      }

      PartitionKey pk = new PartitionKey(key);
      CosmosItemResponse<ObjectNode> response = container.patchItem(key, pk, cosmosPatchOperations, ObjectNode.class);
      if (diagnosticsLatencyThresholdInMS > 0 &&
          response.getDiagnostics().getDuration().compareTo(Duration.ofMillis(diagnosticsLatencyThresholdInMS)) > 0) {
        LOGGER.warn(PATCH_DIAGNOSTIC, response.getDiagnostics().toString());
      }

      if (updateSuccessLatencyTimer != null) {
        long en = System.nanoTime();
        long latency = (en - st) / 1000;
        updateSuccessLatencyTimer.record(latency, TimeUnit.MICROSECONDS);
        updateSuccessCounter.increment();
      }
      return Status.OK;
    } catch (CosmosException e) {
      int statusCode = e.getStatusCode();
      if (!AzureCosmosClient.includeExceptionStackInLog) {
        e = null;
      }
      LOGGER.error(PATCH_EXCEPTION, "Failed to update key {} to collection {} in database {} statusCode {}", key, table,
          AzureCosmosClient.databaseName, statusCode, e);
    }

    if (updateFailureCounter != null) {
      updateFailureCounter.increment();
    }
    return Status.ERROR;
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record key.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Insert key: {} into table: {}", key, table);
    }
    long st = System.nanoTime();
    try {
      CosmosContainer container = AzureCosmosClient.containerCache.get(table);
      if (container == null) {
        container = AzureCosmosClient.database.getContainer(table);
        AzureCosmosClient.containerCache.put(table, container);
      }
      PartitionKey pk = new PartitionKey(key);
      ObjectNode node = OBJECT_MAPPER.createObjectNode();

      node.put("id", key);

      for (Map.Entry<String, ByteIterator> pair : values.entrySet()) {
        node.put(pair.getKey(), pair.getValue().toString());
      }
      CosmosItemResponse<ObjectNode> response;
      if (AzureCosmosClient.useUpsert) {
        response = container.upsertItem(node, pk, new CosmosItemRequestOptions());
      } else {
        response = container.createItem(node, pk, new CosmosItemRequestOptions());
      }

      if (diagnosticsLatencyThresholdInMS > 0 &&
          response.getDiagnostics().getDuration().compareTo(Duration.ofMillis(diagnosticsLatencyThresholdInMS)) > 0) {
        LOGGER.warn(CREATE_DIAGNOSTIC, response.getDiagnostics().toString());
      }

      if (writeSuccessLatencyTimer != null) {
        long en = System.nanoTime();
        long latency = (en - st) / 1000;
        writeSuccessLatencyTimer.record(latency, TimeUnit.MICROSECONDS);
        writeSuccessCounter.increment();
      }
      return Status.OK;
    } catch (CosmosException e) {
      int statusCode = e.getStatusCode();
      if (!AzureCosmosClient.includeExceptionStackInLog) {
        e = null;
      }
      LOGGER.error(CREATE_EXCEPTION, "Failed to insert key {} to collection {} in database {} statusCode {}", key,
          table, AzureCosmosClient.databaseName, statusCode, e);
    }
    if (writeFailureCounter != null) {
      writeFailureCounter.increment();
    }
    return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Delete key {} from table {}", key, table);
    }
    try {
      CosmosContainer container = AzureCosmosClient.containerCache.get(table);
      if (container == null) {
        container = AzureCosmosClient.database.getContainer(table);
        AzureCosmosClient.containerCache.put(table, container);
      }
      CosmosItemResponse<Object> response = container.deleteItem(key,
          new PartitionKey(key),
          new CosmosItemRequestOptions());
      if (diagnosticsLatencyThresholdInMS > 0 &&
          response.getDiagnostics().getDuration().compareTo(Duration.ofMillis(diagnosticsLatencyThresholdInMS)) > 0) {
        LOGGER.warn(DELETE_DIAGNOSTIC, response.getDiagnostics().toString());
      }

      return Status.OK;
    } catch (CosmosException e) {
      int statusCode = e.getStatusCode();
      if (!AzureCosmosClient.includeExceptionStackInLog) {
        e = null;
      }
      LOGGER.error(DELETE_EXCEPTION, "Failed to delete key {} in collection {} database {} statusCode {}", key, table,
          AzureCosmosClient.databaseName, statusCode, e);
    }
    return Status.ERROR;
  }

  private String createSelectTop(Set<String> fields, int top) {
    if (fields == null) {
      return "SELECT TOP " + top + " * ";
    } else {
      StringBuilder result = new StringBuilder("SELECT TOP ").append(top).append(" ");
      int initLength = result.length();
      for (String field : fields) {
        if (result.length() != initLength) {
          result.append(", ");
        }
        result.append("r['").append(field).append("'] ");
      }
      return result.toString();
    }
  }

  private void registerMeter() {
    if (this.getDoubleProperty("readproportion", 0) > 0) {
      readSuccessCounter = Metrics.globalRegistry.counter("Read Successful Operations");
      readFailureCounter = Metrics.globalRegistry.counter("Read Unsuccessful Operations");
      readSuccessLatencyTimer = Timer.builder("Read Successful Latency")
          .publishPercentiles(0.5, 0.95, 0.99, 0.999, 0.9999)
          .publishPercentileHistogram()
          .register(Metrics.globalRegistry);
    }

    if (this.getDoubleProperty("insertproportion", 0) > 0) {
      writeSuccessCounter = Metrics.globalRegistry.counter("Write Successful Operations");
      writeFailureCounter = Metrics.globalRegistry.counter("Write Unsuccessful Operations");
      writeSuccessLatencyTimer = Timer.builder("Write Successful Latency")
          .publishPercentiles(0.5, 0.95, 0.99, 0.999, 0.9999)
          .publishPercentileHistogram()
          .register(Metrics.globalRegistry);
    }

    if (this.getDoubleProperty("scanproportion", 0) > 0) {
      scanSuccessCounter = Metrics.globalRegistry.counter("Scan Successful Operations");
      scanFailureCounter = Metrics.globalRegistry.counter("Scan Unsuccessful Operations");
      scanSuccessLatencyTimer = Timer.builder("Scan Successful Latency")
          .publishPercentiles(0.5, 0.95, 0.99, 0.999, 0.9999)
          .publishPercentileHistogram()
          .register(Metrics.globalRegistry);
    }

    if (this.getDoubleProperty("updateproportion", 0) > 0) {
      updateSuccessCounter = Metrics.globalRegistry.counter("Update Successful Operations");
      updateFailureCounter = Metrics.globalRegistry.counter("Update Unsuccessful Operations");
      updateSuccessLatencyTimer = Timer.builder("Update Successful Latency")
          .publishPercentiles(0.5, 0.95, 0.99, 0.999, 0.9999)
          .publishPercentileHistogram()
          .register(Metrics.globalRegistry);
    }
  }
}