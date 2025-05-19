/*
 * Copyright 2012 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Copyright 2015-2016 YCSB Contributors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package site.ycsb.db;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import site.ycsb.*;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.net.URI;
import java.util.*;
import java.util.Map.Entry;

/**
 * DynamoDB client for YCSB.
 */

public class DynamoDBClient extends DB {

  /**
   * Defines the primary key type used in this particular DB instance.
   * <p>
   * By default, the primary key type is "HASH". Optionally, the user can
   * choose to use hash_and_range key type. See documentation in the
   * DynamoDB.Properties file for more details.
   */
  private enum PrimaryKeyType {
    HASH,
    HASH_AND_RANGE
  }

  private volatile DynamoDbClient dynamoDbClient;
  private String primaryKeyName;
  private PrimaryKeyType primaryKeyType = PrimaryKeyType.HASH;

  // If the user choose to use HASH_AND_RANGE as primary key type, then
  // the following two variables become relevant. See documentation in the
  // DynamoDB.Properties file for more details.
  private String hashKeyValue;
  private String hashKeyName;

  private boolean useBatchInsert = false;
  private boolean consistentRead = false;
  private URI endpoint = null;
  private static final Logger LOGGER = Logger.getLogger(DynamoDBClient.class);
  private static final Status CLIENT_ERROR = new Status("CLIENT_ERROR", "An error occurred on the client.");
  private static final String DEFAULT_HASH_KEY_VALUE = "YCSB_0";
  private static final int DYNAMODB_BATCH_WRITE_MAX_SIZE = 25;
  private final Map<String, List<WriteRequest>> batchInsertItemBuffer = new HashMap<>();

  @Override
  public void init() throws DBException {
    String debug = getProperties().getProperty("dynamodb.debug", null);

    if (null != debug && "true".equalsIgnoreCase(debug)) {
      LOGGER.setLevel(Level.DEBUG);
    }

    String batchInsert = getProperties().getProperty("dynamodb.batchInsert", null);
    String primaryKey = getProperties().getProperty("dynamodb.primaryKey", null);
    String primaryKeyTypeString = getProperties().getProperty("dynamodb.primaryKeyType", null);
    String consistentReads = getProperties().getProperty("dynamodb.consistentReads", null);

    if (null != batchInsert && "true".equalsIgnoreCase(batchInsert)) {
      this.useBatchInsert = true;
    }

    if (null != consistentReads && "true".equalsIgnoreCase(consistentReads)) {
      this.consistentRead = true;
    }

    if (null == primaryKey || primaryKey.length() < 1) {
      throw new DBException("Missing primary key attribute name, cannot continue");
    }
    this.primaryKeyName = primaryKey;

    if (null != primaryKeyTypeString) {
      try {
        this.primaryKeyType = PrimaryKeyType.valueOf(primaryKeyTypeString.trim().toUpperCase());
      } catch (IllegalArgumentException e) {
        throw new DBException("Invalid primary key mode specified: " + primaryKeyTypeString +
            ". Expecting HASH or HASH_AND_RANGE.");
      }
    }

    if (this.primaryKeyType == PrimaryKeyType.HASH_AND_RANGE) {
      // When the primary key type is HASH_AND_RANGE, keys used by YCSB
      // are range keys so we can benchmark performance of individual hash
      // partitions. In this case, the user must specify the hash key's name
      // and optionally can designate a value for the hash key.

      String configuredHashKeyName = getProperties().getProperty("dynamodb.hashKeyName", null);
      if (null == configuredHashKeyName || configuredHashKeyName.isEmpty()) {
        throw new DBException("Must specify a non-empty hash key name when the primary key type is HASH_AND_RANGE.");
      }
      this.hashKeyName = configuredHashKeyName;
      this.hashKeyValue = getProperties().getProperty("dynamodb.hashKeyValue", DEFAULT_HASH_KEY_VALUE);
    }

    // only create one DynamoDB client, used by all YCSB client threads
    if (dynamoDbClient == null) {
      synchronized (DynamoDbClient.class) {
        if (dynamoDbClient == null) {
          DynamoDbClientBuilder dynamoDbClientBuilder = DynamoDbClient.builder();

          Region region = Region.US_EAST_1;
          String configuredRegion = getProperties().getProperty("dynamodb.region", null);
          if (configuredRegion != null) {
            region = Region.of(configuredRegion);
          }
          dynamoDbClientBuilder.region(region);

          String configuredEndpoint = getProperties().getProperty("dynamodb.endpoint", null);
          if (configuredEndpoint != null) {
            dynamoDbClientBuilder.endpointOverride(URI.create(configuredEndpoint));
          }

          // we create the same number of HTTP threads as there are YCSB threads. YCSB default is "1"
          String configuredThreadCount = getProperties().getProperty(Client.THREAD_COUNT_PROPERTY, "1");

          dynamoDbClientBuilder.httpClient(ApacheHttpClient.builder()
              .maxConnections(Integer.parseInt(configuredThreadCount))
              .tcpKeepAlive(true)
              .build());
          this.dynamoDbClient = dynamoDbClientBuilder.build();
        }
      }
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("readkey: " + key + " from table: " + table);
    }

    GetItemRequest req = GetItemRequest.builder()
        .attributesToGet(fields)
        .consistentRead(consistentRead)
        .key(createPrimaryKey(key))
        .tableName(table)
        .build();

    GetItemResponse res;
    try {
      res = dynamoDbClient.getItem(req);
    } catch (AwsServiceException ex) {
      LOGGER.error(ex);
      return Status.ERROR;
    } catch (SdkClientException ex) {
      LOGGER.error(ex);
      return CLIENT_ERROR;
    }

    if (res.hasItem()) {
      result.putAll(extractResult(res.item()));
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Result: " + res.toString());
      }
    }
    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("scan " + recordcount + " records from key: " + startkey + " on table: " + table);
    }

    /*
     * on DynamoDB's scan, startkey is *exclusive* so we need to
     * getItem(startKey) and then use scan for the res
    */
    GetItemRequest greq = GetItemRequest.builder()
        .attributesToGet(fields)
        .key(createPrimaryKey(startkey))
        .tableName(table)
        .build();

    GetItemResponse gres;
    try {
      gres = dynamoDbClient.getItem(greq);
    } catch (AwsServiceException ex) {
      LOGGER.error(ex);
      return Status.ERROR;
    } catch (SdkClientException ex) {
      LOGGER.error(ex);
      return CLIENT_ERROR;
    }

    if (gres.hasItem()) {
      result.add(extractResult(gres.item()));
    }

    int count = 1; // startKey is done, rest to go.

    Map<String, AttributeValue> startKey = createPrimaryKey(startkey);
    ScanRequest.Builder scanRequestBuilder = ScanRequest.builder()
        .attributesToGet(fields)
        .tableName(table);
    while (count < recordcount) {
      scanRequestBuilder.exclusiveStartKey(startKey);
      scanRequestBuilder.limit(recordcount - count);
      ScanResponse res;
      try {
        res = dynamoDbClient.scan(scanRequestBuilder.build());

      } catch (AwsServiceException ex) {
        LOGGER.error(ex);
        return Status.ERROR;
      } catch (SdkClientException ex) {
        LOGGER.error(ex);
        return CLIENT_ERROR;
      }

      count += res.count();
      for (Map<String, AttributeValue> items : res.items()) {
        result.add(extractResult(items));
      }
      startKey = res.lastEvaluatedKey();
    }

    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("updatekey: " + key + " from table: " + table);
    }

    Map<String, AttributeValueUpdate> attributes = new HashMap<>(values.size());
    for (Entry<String, ByteIterator> val : values.entrySet()) {
      AttributeValue v = AttributeValue.fromS(val.getValue().toString());
      attributes.put(val.getKey(), AttributeValueUpdate.builder().action(AttributeAction.PUT).value(v).build());
    }

    UpdateItemRequest req = UpdateItemRequest.builder()
        .attributeUpdates(attributes)
        .key(createPrimaryKey(key))
        .tableName(table)
        .build();

    try {
      dynamoDbClient.updateItem(req);
    } catch (AwsServiceException ex) {
      LOGGER.error(ex);
      return Status.ERROR;
    } catch (SdkClientException ex) {
      LOGGER.error(ex);
      return CLIENT_ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("insertkey: " + primaryKeyName + "-" + key + " from table: " + table);
    }

    Map<String, AttributeValue> attributes = createAttributes(values);
    // adding primary key
    attributes.put(primaryKeyName, AttributeValue.fromS(key));
    if (primaryKeyType == PrimaryKeyType.HASH_AND_RANGE) {
      // If the primary key type is HASH_AND_RANGE, then what has been put
      // into the attributes map above is the range key part of the primary
      // key, we still need to put in the hash key part here.
      attributes.put(hashKeyName, AttributeValue.fromS(hashKeyValue));
    }

    if (useBatchInsert)  {
      WriteRequest writeRequest = WriteRequest.builder()
          .putRequest(PutRequest.builder()
                  .item(attributes)
                  .build()).build();
      if (batchInsertItemBuffer.containsKey(table)) {
        batchInsertItemBuffer.get(table).add(writeRequest);
      } else {
        batchInsertItemBuffer.put(table, new ArrayList<>(Arrays.asList(writeRequest)));
      }
      if (batchInsertItemBuffer.values().stream().mapToInt(List::size).sum() == DYNAMODB_BATCH_WRITE_MAX_SIZE) {
        batchInsertItems();
      }
    } else {
      PutItemRequest putItemRequest = PutItemRequest.builder().item(attributes).tableName(table).build();
      try {
        dynamoDbClient.putItem(putItemRequest);
      } catch (AwsServiceException ex) {
        LOGGER.error(ex);
        return Status.ERROR;
      } catch (SdkClientException ex) {
        LOGGER.error(ex);
        ex.printStackTrace();
        return CLIENT_ERROR;
      }
    }
    return Status.OK;
  }

  private Status batchInsertItems() {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("batch write of " + batchInsertItemBuffer.values().stream().mapToInt(List::size).sum() + " items");
    }
    if (batchInsertItemBuffer.values().stream().mapToInt(List::size).sum() > 0) {
      try {
        boolean allItemsInserted = false;
        int writeAttempts = 0;

        // try to put the items in the DynamoDB table
        // retry with an exponential backoff if the DynamoDB client
        // could not process all items successfully
        while (!allItemsInserted && writeAttempts <= 5) {
          BatchWriteItemResponse response = dynamoDbClient.batchWriteItem(BatchWriteItemRequest.builder()
              .requestItems(batchInsertItemBuffer)
              .build());
          writeAttempts++;
          batchInsertItemBuffer.clear();
          if (response.hasUnprocessedItems() &&
              response.unprocessedItems().values().stream().mapToInt(List::size).sum() > 0) {
            batchInsertItemBuffer.putAll(response.unprocessedItems());
            try {
              Thread.sleep(250 * (int)Math.pow(2, writeAttempts));
            } catch (InterruptedException ie) {
              if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("batchWriteItem sleep for exponential backoff & retry got interrupted");
              }
            }
          } else {
            allItemsInserted = true;
          }
        }
        if (!allItemsInserted) {
          LOGGER.error("Client failed to insert all the items in the batch");
          return CLIENT_ERROR;
        }
      } catch (AwsServiceException ex) {
        LOGGER.error(ex);
        return Status.ERROR;
      } catch (SdkClientException ex) {
        LOGGER.error(ex);
        return CLIENT_ERROR;
      }
    }
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("deletekey: " + key + " from table: " + table);
    }

    DeleteItemRequest req = DeleteItemRequest.builder().key(createPrimaryKey(key)).tableName(table).build();

    try {
      dynamoDbClient.deleteItem(req);
    } catch (AwsServiceException ex) {
      LOGGER.error(ex);
      return Status.ERROR;
    } catch (SdkClientException ex) {
      LOGGER.error(ex);
      return CLIENT_ERROR;
    }
    return Status.OK;
  }

  /**
   * Cleanup any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    if (useBatchInsert) {
      // flush any remaining items to the DynamoDB table
      batchInsertItems();
    }
  }

  private static Map<String, AttributeValue> createAttributes(Map<String, ByteIterator> values) {
    Map<String, AttributeValue> attributes = new HashMap<>(values.size() + 1);
    for (Entry<String, ByteIterator> val : values.entrySet()) {
      attributes.put(val.getKey(), AttributeValue.fromS(val.getValue().toString()));
    }
    return attributes;
  }

  private HashMap<String, ByteIterator> extractResult(Map<String, AttributeValue> item) {
    if (null == item) {
      return null;
    }
    HashMap<String, ByteIterator> rItems = new HashMap<>(item.size());

    for (Entry<String, AttributeValue> attr : item.entrySet()) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(String.format("Result- key: %s, value: %s", attr.getKey(), attr.getValue()));
      }
      rItems.put(attr.getKey(), new StringByteIterator(attr.getValue().s()));
    }
    return rItems;
  }

  private Map<String, AttributeValue> createPrimaryKey(String key) {
    Map<String, AttributeValue> k = new HashMap<>();
    if (primaryKeyType == PrimaryKeyType.HASH) {
      k.put(primaryKeyName, AttributeValue.fromS(key));
    } else if (primaryKeyType == PrimaryKeyType.HASH_AND_RANGE) {
      k.put(hashKeyName, AttributeValue.fromS(hashKeyValue));
      k.put(primaryKeyName, AttributeValue.fromS(key));
    } else {
      throw new RuntimeException("Assertion Error: impossible primary key type");
    }
    return k;
  }
}
