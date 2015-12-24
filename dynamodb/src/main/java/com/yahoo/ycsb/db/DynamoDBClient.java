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

package com.yahoo.ycsb.db;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.*;
import com.yahoo.ycsb.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;

/**
 * DynamoDB v1.10.48 client for YCSB.
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

  private AmazonDynamoDBClient dynamoDB;
  private String primaryKeyName;
  private PrimaryKeyType primaryKeyType = PrimaryKeyType.HASH;

  // If the user choose to use HASH_AND_RANGE as primary key type, then
  // the following two variables become relevant. See documentation in the
  // DynamoDB.Properties file for more details.
  private String hashKeyValue;
  private String hashKeyName;

  private boolean consistentRead = false;
  private String endpoint = "http://dynamodb.us-east-1.amazonaws.com";
  private int maxConnects = 50;
  private static final Logger LOGGER = Logger.getLogger(DynamoDBClient.class);
  private static final Status CLIENT_ERROR = new Status("CLIENT_ERROR", "An error occurred on the client.");
  private static final String DEFAULT_HASH_KEY_VALUE = "YCSB_0";

  @Override
  public void init() throws DBException {
    String debug = getProperties().getProperty("dynamodb.debug", null);

    if (null != debug && "true".equalsIgnoreCase(debug)) {
      LOGGER.setLevel(Level.DEBUG);
    }

    String configuredEndpoint = getProperties().getProperty("dynamodb.endpoint", null);
    String credentialsFile = getProperties().getProperty("dynamodb.awsCredentialsFile", null);
    String primaryKey = getProperties().getProperty("dynamodb.primaryKey", null);
    String primaryKeyTypeString = getProperties().getProperty("dynamodb.primaryKeyType", null);
    String consistentReads = getProperties().getProperty("dynamodb.consistentReads", null);
    String connectMax = getProperties().getProperty("dynamodb.connectMax", null);

    if (null != connectMax) {
      this.maxConnects = Integer.parseInt(connectMax);
    }

    if (null != consistentReads && "true".equalsIgnoreCase(consistentReads)) {
      this.consistentRead = true;
    }

    if (null != configuredEndpoint) {
      this.endpoint = configuredEndpoint;
    }

    if (null == primaryKey || primaryKey.length() < 1) {
      throw new DBException("Missing primary key attribute name, cannot continue");
    }

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

    try {
      AWSCredentials credentials = new PropertiesCredentials(new File(credentialsFile));
      ClientConfiguration cconfig = new ClientConfiguration();
      cconfig.setMaxConnections(maxConnects);
      dynamoDB = new AmazonDynamoDBClient(credentials, cconfig);
      dynamoDB.setEndpoint(this.endpoint);
      primaryKeyName = primaryKey;
      LOGGER.info("dynamodb connection created with " + this.endpoint);
    } catch (Exception e1) {
      LOGGER.error("DynamoDBClient.init(): Could not initialize DynamoDB client.", e1);
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("readkey: " + key + " from table: " + table);
    }

    GetItemRequest req = new GetItemRequest(table, createPrimaryKey(key));
    req.setAttributesToGet(fields);
    req.setConsistentRead(consistentRead);
    GetItemResult res;

    try {
      res = dynamoDB.getItem(req);
    } catch (AmazonServiceException ex) {
      LOGGER.error(ex);
      return Status.ERROR;
    } catch (AmazonClientException ex) {
      LOGGER.error(ex);
      return CLIENT_ERROR;
    }

    if (null != res.getItem()) {
      result.putAll(extractResult(res.getItem()));
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
    GetItemRequest greq = new GetItemRequest(table, createPrimaryKey(startkey));
    greq.setAttributesToGet(fields);

    GetItemResult gres;

    try {
      gres = dynamoDB.getItem(greq);
    } catch (AmazonServiceException ex) {
      LOGGER.error(ex);
      return Status.ERROR;
    } catch (AmazonClientException ex) {
      LOGGER.error(ex);
      return CLIENT_ERROR;
    }

    if (null != gres.getItem()) {
      result.add(extractResult(gres.getItem()));
    }

    int count = 1; // startKey is done, rest to go.

    Map<String, AttributeValue> startKey = createPrimaryKey(startkey);
    ScanRequest req = new ScanRequest(table);
    req.setAttributesToGet(fields);
    while (count < recordcount) {
      req.setExclusiveStartKey(startKey);
      req.setLimit(recordcount - count);
      ScanResult res;
      try {
        res = dynamoDB.scan(req);
      } catch (AmazonServiceException ex) {
        LOGGER.error(ex);
        return Status.ERROR;
      } catch (AmazonClientException ex) {
        LOGGER.error(ex);
        return CLIENT_ERROR;
      }

      count += res.getCount();
      for (Map<String, AttributeValue> items : res.getItems()) {
        result.add(extractResult(items));
      }
      startKey = res.getLastEvaluatedKey();

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
      AttributeValue v = new AttributeValue(val.getValue().toString());
      attributes.put(val.getKey(), new AttributeValueUpdate().withValue(v).withAction("PUT"));
    }

    UpdateItemRequest req = new UpdateItemRequest(table, createPrimaryKey(key), attributes);

    try {
      dynamoDB.updateItem(req);
    } catch (AmazonServiceException ex) {
      LOGGER.error(ex);
      return Status.ERROR;
    } catch (AmazonClientException ex) {
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
    attributes.put(primaryKeyName, new AttributeValue(key));
    if (primaryKeyType == PrimaryKeyType.HASH_AND_RANGE) {
      // If the primary key type is HASH_AND_RANGE, then what has been put
      // into the attributes map above is the range key part of the primary
      // key, we still need to put in the hash key part here.
      attributes.put(hashKeyName, new AttributeValue(hashKeyValue));
    }

    PutItemRequest putItemRequest = new PutItemRequest(table, attributes);
    try {
      dynamoDB.putItem(putItemRequest);
    } catch (AmazonServiceException ex) {
      LOGGER.error(ex);
      return Status.ERROR;
    } catch (AmazonClientException ex) {
      LOGGER.error(ex);
      return CLIENT_ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("deletekey: " + key + " from table: " + table);
    }

    DeleteItemRequest req = new DeleteItemRequest(table, createPrimaryKey(key));

    try {
      dynamoDB.deleteItem(req);
    } catch (AmazonServiceException ex) {
      LOGGER.error(ex);
      return Status.ERROR;
    } catch (AmazonClientException ex) {
      LOGGER.error(ex);
      return CLIENT_ERROR;
    }
    return Status.OK;
  }

  private static Map<String, AttributeValue> createAttributes(Map<String, ByteIterator> values) {
    Map<String, AttributeValue> attributes = new HashMap<>(values.size() + 1);
    for (Entry<String, ByteIterator> val : values.entrySet()) {
      attributes.put(val.getKey(), new AttributeValue(val.getValue().toString()));
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
      rItems.put(attr.getKey(), new StringByteIterator(attr.getValue().getS()));
    }
    return rItems;
  }

  private Map<String, AttributeValue> createPrimaryKey(String key) {
    Map<String, AttributeValue> k = new HashMap<>();
    if (primaryKeyType == PrimaryKeyType.HASH) {
      k.put(primaryKeyName, new AttributeValue().withS(key));
    } else if (primaryKeyType == PrimaryKeyType.HASH_AND_RANGE) {
      k.put(hashKeyName, new AttributeValue().withS(hashKeyValue));
      k.put(primaryKeyName, new AttributeValue().withS(key));
    } else {
      throw new RuntimeException("Assertion Error: impossible primary key type");
    }
    return k;
  }
}
