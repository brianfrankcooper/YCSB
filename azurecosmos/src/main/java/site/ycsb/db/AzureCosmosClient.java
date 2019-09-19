/*
 * Copyright (c) 2018 YCSB contributors. All rights reserved.
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

// Authors: Anthony F. Voellm and Khoa Dang

package site.ycsb.db;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.documentdb.*;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

/**
 * Azure Cosmos DB v1.00 client for YCSB.
 */

public class AzureCosmosClient extends DB {
  // Document uri naming
  private static final String DATABASES_PATH_SEGMENT = "dbs";
  private static final String COLLECTIONS_PATH_SEGMENT = "colls";
  private static final String DOCUMENTS_PATH_SEGMENT = "docs";

  // Default configuration values
  private static final String DEFAULT_CONSISTENCY_LEVEL = "Session";
  private static final String DEFAULT_DATABASE_NAME = "ycsb";
  private static final String DEFAULT_CONNECTION_MODE = "DirectHttps";
  private static final boolean DEFAULT_USE_UPSERT = false;
  private static final int DEFAULT_MAX_DEGREE_OF_PARALLELISM_FOR_QUERY = 0;
  private static final boolean DEFAULT_INCLUDE_EXCEPTION_STACK_IN_LOG = false;

  private static final Logger LOGGER = LoggerFactory.getLogger(AzureCosmosClient.class);

  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  private static DocumentClient client;
  private String databaseName;
  private boolean useUpsert;
  private int maxDegreeOfParallelismForQuery;
  private boolean includeExceptionStackInLog;

  @Override
  public synchronized void init() throws DBException {
    INIT_COUNT.incrementAndGet();
    if (client != null) {
      return;
    }

    initAzureCosmosClient();
  }

  private void initAzureCosmosClient() throws DBException {
    // Connection properties
    String primaryKey = this.getStringProperty("azurecosmos.primaryKey", null);
    if (StringUtils.isEmpty(primaryKey)) {
      throw new DBException("Missing primary key required to connect to the database.");
    }

    String uri = getProperties().getProperty("azurecosmos.uri", null);
    if (StringUtils.isEmpty(uri)) {
      throw new DBException("Missing uri required to connect to the database.");
    }

    this.useUpsert = this.getBooleanProperty(
            "azurecosmos.useUpsert",
            DEFAULT_USE_UPSERT);

    this.databaseName = this.getStringProperty(
            "azurecosmos.databaseName",
            DEFAULT_DATABASE_NAME);

    this.maxDegreeOfParallelismForQuery = this.getIntProperty(
            "azurecosmos.maxDegreeOfParallelismForQuery",
            DEFAULT_MAX_DEGREE_OF_PARALLELISM_FOR_QUERY);

    this.includeExceptionStackInLog = this.getBooleanProperty(
            "azurecosmos.includeExceptionStackInLog",
            DEFAULT_INCLUDE_EXCEPTION_STACK_IN_LOG);

    ConsistencyLevel consistencyLevel = ConsistencyLevel.valueOf(this.getStringProperty(
            "azurecosmos.consistencyLevel",
            DEFAULT_CONSISTENCY_LEVEL));
    String connectionModeString = this.getStringProperty(
            "azurecosmos.connectionMode",
            DEFAULT_CONNECTION_MODE);

    ConnectionPolicy connectionPolicy = new ConnectionPolicy();
    connectionPolicy.setEnableEndpointDiscovery(false);
    connectionPolicy.setConnectionMode(ConnectionMode.valueOf(connectionModeString));
    connectionPolicy.setMaxPoolSize(this.getIntProperty("azurecosmos.maxConnectionPoolSize",
                                                        connectionPolicy.getMaxPoolSize()));
    connectionPolicy.setIdleConnectionTimeout(this.getIntProperty("azurecosmos.idleConnectionTimeout",
                                                                  connectionPolicy.getIdleConnectionTimeout()));

    RetryOptions retryOptions = new RetryOptions();
    retryOptions.setMaxRetryAttemptsOnThrottledRequests(this.getIntProperty(
            "azurecosmos.maxRetryAttemptsOnThrottledRequests",
            retryOptions.getMaxRetryAttemptsOnThrottledRequests()));
    retryOptions.setMaxRetryWaitTimeInSeconds(this.getIntProperty(
              "azurecosmos.maxRetryWaitTimeInSeconds",
              retryOptions.getMaxRetryWaitTimeInSeconds()));
    connectionPolicy.setRetryOptions(retryOptions);

    try {
      LOGGER.info("Creating azurecosmos client {}.. connectivityMode={}, consistencyLevel={},"
                      + " maxRetryAttemptsOnThrottledRequests={}, maxRetryWaitTimeInSeconds={}"
                      + " useUpsert={}",
              uri,
              connectionPolicy.getConnectionMode(),
              consistencyLevel.toString(),
              connectionPolicy.getRetryOptions().getMaxRetryAttemptsOnThrottledRequests(),
              connectionPolicy.getRetryOptions().getMaxRetryWaitTimeInSeconds(),
              this.useUpsert);
      AzureCosmosClient.client = new DocumentClient(uri, primaryKey, connectionPolicy, consistencyLevel);
      LOGGER.info("Azure Cosmos connection created: {}", uri);
    } catch (IllegalArgumentException e) {
      throw new DBException("Illegal argument passed in.  Check the format of your parameters.", e);
    }

    // Verify the database exists
    try {
      AzureCosmosClient.client.readDatabase(getDatabaseLink(this.databaseName), new RequestOptions());
    } catch (DocumentClientException e) {
      throw new DBException("Invalid database name (" + this.databaseName + ") or failed to read database.", e);
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

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    if (INIT_COUNT.decrementAndGet() == 0) {
      try {
        AzureCosmosClient.client.close();
      } catch (Exception e) {
        if (!this.includeExceptionStackInLog) {
          e = null;
        }
        LOGGER.error("Could not close DocumentClient", e);
      } finally {
        client = null;
      }
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    String documentLink = getDocumentLink(this.databaseName, table, key);

    ResourceResponse<Document> readResource = null;
    Document document = null;

    try {
      readResource = AzureCosmosClient.client.readDocument(documentLink, getRequestOptions(key));
      document = readResource.getResource();
    } catch (DocumentClientException e) {
      if (!this.includeExceptionStackInLog) {
        e = null;
      }
      LOGGER.error("Failed to read key {} in collection {} in database {}", key, table, this.databaseName, e);
      return Status.ERROR;
    }

    if (document != null) {
      result.putAll(extractResult(document));
    }

    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    List<Document> documents;
    FeedResponse<Document> feedResponse = null;
    try {
      FeedOptions feedOptions = new FeedOptions();
      feedOptions.setEnableCrossPartitionQuery(true);
      feedOptions.setMaxDegreeOfParallelism(this.maxDegreeOfParallelismForQuery);
      feedResponse = AzureCosmosClient.client.queryDocuments(getDocumentCollectionLink(this.databaseName, table),
            new SqlQuerySpec("SELECT TOP @recordcount * FROM root r WHERE r.id >= @startkey",
                    new SqlParameterCollection(new SqlParameter("@recordcount", recordcount),
                                               new SqlParameter("@startkey", startkey))),
                    feedOptions);
      documents = feedResponse.getQueryIterable().toList();
    } catch (Exception e) {
      if (!this.includeExceptionStackInLog) {
        e = null;
      }
      LOGGER.error("Failed to scan with startKey={}, recordCount={}", startkey, recordcount, e);
      return Status.ERROR;
    }

    if (documents != null) {
      for (Document document : documents) {
        result.add(this.extractResult(document));
      }
    }

    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    // Azure Cosmos does not have patch support.  Until then we need to read
    // the document, update in place, and then write back.
    // This could actually be made more efficient by using a stored procedure
    // and doing the read/modify write on the server side.  Perhaps
    // that will be a future improvement.

    String documentLink = getDocumentLink(this.databaseName, table, key);
    ResourceResponse<Document> updatedResource = null;
    ResourceResponse<Document> readResouce = null;
    RequestOptions reqOptions = null;
    Document document = null;

    try {
      reqOptions = getRequestOptions(key);
      readResouce = AzureCosmosClient.client.readDocument(documentLink, reqOptions);
      document = readResouce.getResource();
    } catch (DocumentClientException e) {
      if (!this.includeExceptionStackInLog) {
        e = null;
      }
      LOGGER.error("Failed to read key {} in collection {} in database {} during update operation",
          key, table, this.databaseName, e);
      return Status.ERROR;
    }

    // Update values
    for (Entry<String, ByteIterator> entry : values.entrySet()) {
      document.set(entry.getKey(), entry.getValue().toString());
    }

    AccessCondition accessCondition = new AccessCondition();
    accessCondition.setCondition(document.getETag());
    accessCondition.setType(AccessConditionType.IfMatch);
    reqOptions.setAccessCondition(accessCondition);

    try {
      updatedResource = AzureCosmosClient.client.replaceDocument(documentLink, document, reqOptions);
    } catch (DocumentClientException e) {
      if (!this.includeExceptionStackInLog) {
        e = null;
      }
      LOGGER.error("Failed to update key {}", key, e);
      return Status.ERROR;
    }
  
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    Document documentDefinition = getDocumentDefinition(key, values);
    ResourceResponse<Document> resourceResponse = null;
    RequestOptions requestOptions = getRequestOptions(key);
  
    try {
      if (this.useUpsert) {
        resourceResponse = AzureCosmosClient.client.upsertDocument(getDocumentCollectionLink(this.databaseName, table),
            documentDefinition,
            requestOptions,
            true);
      } else {
        resourceResponse = AzureCosmosClient.client.createDocument(getDocumentCollectionLink(this.databaseName, table),
            documentDefinition,
            requestOptions,
            true);
      }

    } catch (DocumentClientException e) {
      if (!this.includeExceptionStackInLog) {
        e = null;
      }
      LOGGER.error("Failed to insert key {} to collection {} in database {}", key, table, this.databaseName, e);
      return Status.ERROR;
    }

    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    ResourceResponse<Document> deletedResource = null;

    try {
      deletedResource = AzureCosmosClient.client.deleteDocument(getDocumentLink(this.databaseName, table, key),
              getRequestOptions(key));
    } catch (DocumentClientException e) {
      if (!this.includeExceptionStackInLog) {
        e = null;
      }
      LOGGER.error("Failed to delete key {} in collection {} in database {}", key, table, this.databaseName, e);
      return Status.ERROR;
    }

    return Status.OK;
  }

  private HashMap<String, ByteIterator> extractResult(Document item) {
    if (null == item) {
      return null;
    }
    HashMap<String, ByteIterator> rItems = new HashMap<>(item.getHashMap().size());

    for (Entry<String, Object> attr : item.getHashMap().entrySet()) {
      rItems.put(attr.getKey(), new StringByteIterator(attr.getValue().toString()));
    }
    return rItems;
  }

  private RequestOptions getRequestOptions(String key) {
    RequestOptions requestOptions = new RequestOptions();
    requestOptions.setPartitionKey(new PartitionKey(key));
    return requestOptions;
  }

  private static String getDatabaseLink(String databaseName) {
    return String.format("%s/%s", DATABASES_PATH_SEGMENT, databaseName);
  }

  private static String getDocumentCollectionLink(String databaseName, String table) {
    return String.format("%s/%s/%s",
            getDatabaseLink(databaseName),
            COLLECTIONS_PATH_SEGMENT,
            table);
  }

  private static String getDocumentLink(String databaseName, String table, String key) {
    return String.format("%s/%s/%s",
            getDocumentCollectionLink(databaseName, table),
            DOCUMENTS_PATH_SEGMENT,
            key);
  }

  private Document getDocumentDefinition(String key, Map<String, ByteIterator> values) {
    Document document = new Document();
    document.set("id", key);
    for (Entry<String, ByteIterator> entry : values.entrySet()) {
      document.set(entry.getKey(), entry.getValue().toString());
    }
    return document;
  }
}

