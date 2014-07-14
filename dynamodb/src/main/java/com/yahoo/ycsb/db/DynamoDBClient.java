/*

 * Copyright 2012 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import java.util.*;
import java.util.Map.Entry;
import java.io.File;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodb.model.DeleteItemRequest;
import com.amazonaws.services.dynamodb.model.DeleteItemResult;
import com.amazonaws.services.dynamodb.model.GetItemRequest;
import com.amazonaws.services.dynamodb.model.GetItemResult;
import com.amazonaws.services.dynamodb.model.Key;
import com.amazonaws.services.dynamodb.model.PutItemRequest;
import com.amazonaws.services.dynamodb.model.PutItemResult;
import com.amazonaws.services.dynamodb.model.ScanRequest;
import com.amazonaws.services.dynamodb.model.ScanResult;
import com.amazonaws.services.dynamodb.model.UpdateItemRequest;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

/**
 * DynamoDB v1.3.14 client for YCSB
 */

public class DynamoDBClient extends DB {

    private static final int OK = 0;
    private static final int SERVER_ERROR = 1;
    private static final int CLIENT_ERROR = 2;
    private AmazonDynamoDBClient dynamoDB;
    private String primaryKeyName;
    private boolean debug = false;
    private boolean consistentRead = false;
    private String endpoint = "http://dynamodb.us-east-1.amazonaws.com";
    private int maxConnects = 50;
    private static Logger logger = Logger.getLogger(DynamoDBClient.class);
    public DynamoDBClient() {}

    /**
     * Initialize any state for this DB. Called once per DB instance; there is
     * one DB instance per client thread.
     */
    public void init() throws DBException {
        // initialize DynamoDb driver & table.
        String debug = getProperties().getProperty("dynamodb.debug",null);

        if (null != debug && "true".equalsIgnoreCase(debug)) {
            logger.setLevel(Level.DEBUG);
        }

        String endpoint = getProperties().getProperty("dynamodb.endpoint",null);
        String credentialsFile = getProperties().getProperty("dynamodb.awsCredentialsFile",null);
        String primaryKey = getProperties().getProperty("dynamodb.primaryKey",null);
        String consistentReads = getProperties().getProperty("dynamodb.consistentReads",null);
        String connectMax = getProperties().getProperty("dynamodb.connectMax",null);

        if (null != connectMax) {
            this.maxConnects = Integer.parseInt(connectMax);
        }

        if (null != consistentReads && "true".equalsIgnoreCase(consistentReads)) {
            this.consistentRead = true;
        }

        if (null != endpoint) {
            this.endpoint = endpoint;
        }

        if (null == primaryKey || primaryKey.length() < 1) {
            String errMsg = "Missing primary key attribute name, cannot continue";
            logger.error(errMsg);
        }

        try {
            AWSCredentials credentials = new PropertiesCredentials(new File(credentialsFile));
            ClientConfiguration cconfig = new ClientConfiguration();
            cconfig.setMaxConnections(maxConnects);
            dynamoDB = new AmazonDynamoDBClient(credentials,cconfig);
            dynamoDB.setEndpoint(this.endpoint);
            primaryKeyName = primaryKey;
            logger.info("dynamodb connection created with " + this.endpoint);
        } catch (Exception e1) {
            String errMsg = "DynamoDBClient.init(): Could not initialize DynamoDB client: " + e1.getMessage();
            logger.error(errMsg);
        }
    }

    @Override
    public int readOne(String table, String key, String field, Map<String,ByteIterator> result) {

        GetItemRequest req = new GetItemRequest(table, createPrimaryKey(key));
        req.setAttributesToGet(Collections.singleton(field));
        req.setConsistentRead(consistentRead);

        return read(table, key, result, req);
    }

    @Override
    public int readAll(String table, String key, Map<String,ByteIterator> result) {

        GetItemRequest req = new GetItemRequest(table, createPrimaryKey(key));
        req.setAttributesToGet(null);
        req.setConsistentRead(consistentRead);

        return read(table, key, result, req);
    }

    public int read(String table, String key, Map<String, ByteIterator> result,
                    GetItemRequest req) {

        logger.debug("readkey: " + key + " from table: " + table);
        GetItemResult res = null;

        try {
            res = dynamoDB.getItem(req);
        }catch (AmazonServiceException ex) {
            logger.error(ex.getMessage());
            return SERVER_ERROR;
        }catch (AmazonClientException ex){
            logger.error(ex.getMessage());
            return CLIENT_ERROR;
        }

        if (null != res.getItem())
        {
            result.putAll(extractResult(res.getItem()));
            logger.debug("Result: " + res.toString());
        }
        return OK;
    }

    @Override
    public int scanOne(String table, String startkey, int recordcount, String field,
                       List<Map<String, ByteIterator>> result) {

        GetItemRequest greq = new GetItemRequest(table, createPrimaryKey(startkey));
        greq.setAttributesToGet(Collections.singleton(field));

        ScanRequest req = new ScanRequest(table);
        req.setAttributesToGet(Collections.singleton(field));

        return scan(table, startkey, recordcount, result, greq, req);
    }

    @Override
    public int scanAll(String table, String startkey, int recordcount, List<Map<String, ByteIterator>> result) {

        GetItemRequest greq = new GetItemRequest(table, createPrimaryKey(startkey));
        greq.setAttributesToGet(null);

        ScanRequest req = new ScanRequest(table);
        req.setAttributesToGet(null);

        return scan(table, startkey, recordcount, result, greq, req);
    }

    /*
     * on DynamoDB's scan, startkey is *exclusive* so we need to
     * getItem(startKey) and then use scan for the res
     */
    public int scan(String table, String startkey, int recordcount, List<Map<String, ByteIterator>> result,
                    GetItemRequest greq, ScanRequest req) {
        logger.debug("scan " + recordcount + " records from key: " + startkey + " on table: " + table);
        GetItemResult gres = null;

        try {
            gres = dynamoDB.getItem(greq);
        }catch (AmazonServiceException ex) {
            logger.error(ex.getMessage());
            return SERVER_ERROR;
        }catch (AmazonClientException ex){
            logger.error(ex.getMessage());
           return CLIENT_ERROR;
        }

        if (null != gres.getItem()) {
            result.add(extractResult(gres.getItem()));
        }

        int count = 1; // startKey is done, rest to go.

        Key startKey = createPrimaryKey(startkey);
        while (count < recordcount) {
            req.setExclusiveStartKey(startKey);
            req.setLimit(recordcount - count);
            ScanResult res = null;
            try {
                res = dynamoDB.scan(req);
            }catch (AmazonServiceException ex) {
                logger.error(ex.getMessage());
              ex.printStackTrace();
             return SERVER_ERROR;
            }catch (AmazonClientException ex){
                logger.error(ex.getMessage());
               ex.printStackTrace();
             return CLIENT_ERROR;
            }

            count += res.getCount();
            for (Map<String, AttributeValue> items : res.getItems()) {
                result.add(extractResult(items));
            }
            startKey = res.getLastEvaluatedKey();

        }

        return OK;
    }

    @Override
    public int updateOne(String table, String key, String field, ByteIterator value) {

        Map<String, AttributeValueUpdate> attributes = new HashMap<String, AttributeValueUpdate>(1);
        AttributeValue v = new AttributeValue(value.toString());
        attributes.put(field, new AttributeValueUpdate()
                  .withValue(v).withAction("PUT"));

        return update(table, key, attributes);
    }

    @Override
    public int updateAll(String table, String key, Map<String,ByteIterator> values) {

        Map<String, AttributeValueUpdate> attributes = new HashMap<String, AttributeValueUpdate>(values.size());
        for (Entry<String, ByteIterator> val : values.entrySet()) {
            AttributeValue v = new AttributeValue(val.getValue().toString());
            attributes.put(val.getKey(), new AttributeValueUpdate()
                      .withValue(v).withAction("PUT"));
        }

        return update(table, key, attributes);
    }

    public int update(String table, String key, Map<String, AttributeValueUpdate> attributes) {
        logger.debug("updatekey: " + key + " from table: " + table);

        UpdateItemRequest req = new UpdateItemRequest(table, createPrimaryKey(key), attributes);

        try {
            dynamoDB.updateItem(req);
        }catch (AmazonServiceException ex) {
            logger.error(ex.getMessage());
            return SERVER_ERROR;
        }catch (AmazonClientException ex){
            logger.error(ex.getMessage());
            return CLIENT_ERROR;
        }
        return OK;
    }

    @Override
    public int insert(String table, String key, Map<String, ByteIterator> values) {
        logger.debug("insertkey: " + primaryKeyName + "-" + key + " from table: " + table);
        Map<String, AttributeValue> attributes = createAttributes(values);
        // adding primary key
        attributes.put(primaryKeyName, new AttributeValue(key));

        PutItemRequest putItemRequest = new PutItemRequest(table, attributes);
        PutItemResult res = null;
        try {
            res = dynamoDB.putItem(putItemRequest);
        }catch (AmazonServiceException ex) {
            logger.error(ex.getMessage());
            return SERVER_ERROR;
        }catch (AmazonClientException ex){
            logger.error(ex.getMessage());
            return CLIENT_ERROR;
        }
        return OK;
    }

    @Override
    public int delete(String table, String key) {
        logger.debug("deletekey: " + key + " from table: " + table);
        DeleteItemRequest req = new DeleteItemRequest(table, createPrimaryKey(key));
        DeleteItemResult res = null;

        try {
            res = dynamoDB.deleteItem(req);
        }catch (AmazonServiceException ex) {
            logger.error(ex.getMessage());
            return SERVER_ERROR;
        }catch (AmazonClientException ex){
            logger.error(ex.getMessage());
            return CLIENT_ERROR;
        }
        return OK;
    }

    private static Map<String, AttributeValue> createAttributes(
            Map<String, ByteIterator> values) {
        Map<String, AttributeValue> attributes = new HashMap<String, AttributeValue>(
                values.size() + 1); //leave space for the PrimaryKey
        for (Entry<String, ByteIterator> val : values.entrySet()) {
            attributes.put(val.getKey(), new AttributeValue(val.getValue()
                    .toString()));
        }
        return attributes;
    }

    private HashMap<String, ByteIterator> extractResult(Map<String, AttributeValue> item) {
        if(null == item)
            return null;
        HashMap<String, ByteIterator> rItems = new HashMap<String, ByteIterator>(item.size());

        for (Entry<String, AttributeValue> attr : item.entrySet()) {
            logger.debug(String.format("Result- key: %s, value: %s", attr.getKey(), attr.getValue()) );
            rItems.put(attr.getKey(), new StringByteIterator(attr.getValue().getS()));
        }
        return rItems;
    }

    private static Key createPrimaryKey(String key) {
        Key k = new Key().withHashKeyElement(new AttributeValue().withS(key));
        return k;
    }
}
