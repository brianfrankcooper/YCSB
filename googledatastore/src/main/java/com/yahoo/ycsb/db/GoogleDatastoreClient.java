/*
 * Copyright 2015 YCSB contributors. All Rights Reserved.
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

import com.google.api.client.auth.oauth2.Credential;
import com.google.datastore.v1.*;
import com.google.datastore.v1.CommitRequest.Mode;
import com.google.datastore.v1.ReadOptions.ReadConsistency;
import com.google.datastore.v1.client.Datastore;
import com.google.datastore.v1.client.DatastoreException;
import com.google.datastore.v1.client.DatastoreFactory;
import com.google.datastore.v1.client.DatastoreHelper;
import com.google.datastore.v1.client.DatastoreOptions;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;

import javax.annotation.Nullable;

/**
 * Google Cloud Datastore Client for YCSB.
 */

public class GoogleDatastoreClient extends DB {
  /**
   * Defines a MutationType used in this class.
   */
  private enum MutationType {
    UPSERT,
    UPDATE,
    DELETE
  }

  /**
   * Defines a EntityGroupingMode enum used in this class.
   */
  private enum EntityGroupingMode {
    ONE_ENTITY_PER_GROUP,
    MULTI_ENTITY_PER_GROUP
  }

  private static Logger logger =
      Logger.getLogger(GoogleDatastoreClient.class);

  // Read consistency defaults to "STRONG" per YCSB guidance.
  // User can override this via configure.
  private ReadConsistency readConsistency = ReadConsistency.STRONG;

  private EntityGroupingMode entityGroupingMode =
      EntityGroupingMode.ONE_ENTITY_PER_GROUP;

  private String rootEntityName;

  private Datastore datastore = null;

  /**
   * Initialize any state for this DB. Called once per DB instance; there is
   * one DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    String debug = getProperties().getProperty("googledatastore.debug", null);
    if (null != debug && "true".equalsIgnoreCase(debug)) {
      logger.setLevel(Level.DEBUG);
    }

    // We need the following 3 essential properties to initialize datastore:
    //
    // - DatasetId,
    // - Path to private key file,
    // - Service account email address.
    String datasetId = getProperties().getProperty(
        "googledatastore.datasetId", null);
    if (datasetId == null) {
      throw new DBException(
          "Required property \"datasetId\" missing.");
    }

    String privateKeyFile = getProperties().getProperty(
        "googledatastore.privateKeyFile", null);
    if (privateKeyFile == null) {
      throw new DBException(
          "Required property \"privateKeyFile\" missing.");
    }

    String serviceAccountEmail = getProperties().getProperty(
        "googledatastore.serviceAccountEmail", null);
    if (serviceAccountEmail == null) {
      throw new DBException(
          "Required property \"serviceAccountEmail\" missing.");
    }

    // Below are properties related to benchmarking.

    String readConsistencyConfig = getProperties().getProperty(
        "googledatastore.readConsistency", null);
    if (readConsistencyConfig != null) {
      try {
        this.readConsistency = ReadConsistency.valueOf(
            readConsistencyConfig.trim().toUpperCase());
      } catch (IllegalArgumentException e) {
        throw new DBException("Invalid read consistency specified: " +
            readConsistencyConfig + ". Expecting STRONG or EVENTUAL.");
      }
    }

    //
    // Entity Grouping Mode (googledatastore.entitygroupingmode), see
    // documentation in conf/googledatastore.properties.
    //
    String entityGroupingConfig = getProperties().getProperty(
        "googledatastore.entityGroupingMode", null);
    if (entityGroupingConfig != null) {
      try {
        this.entityGroupingMode = EntityGroupingMode.valueOf(
            entityGroupingConfig.trim().toUpperCase());
      } catch (IllegalArgumentException e) {
        throw new DBException("Invalid entity grouping mode specified: " +
            entityGroupingConfig + ". Expecting ONE_ENTITY_PER_GROUP or " +
            "MULTI_ENTITY_PER_GROUP.");
      }
    }

    this.rootEntityName = getProperties().getProperty(
        "googledatastore.rootEntityName", "YCSB_ROOT_ENTITY");

    try {
      // Setup the connection to Google Cloud Datastore with the credentials
      // obtained from the configure.
      DatastoreOptions.Builder options = new DatastoreOptions.Builder();
      Credential credential = DatastoreHelper.getServiceAccountCredential(
          serviceAccountEmail, privateKeyFile);
      logger.info("Using JWT Service Account credential.");
      logger.info("DatasetID: " + datasetId + ", Service Account Email: " +
          serviceAccountEmail + ", Private Key File Path: " + privateKeyFile);

      datastore = DatastoreFactory.get().create(
          options.credential(credential).projectId(datasetId).build());

    } catch (GeneralSecurityException exception) {
      throw new DBException("Security error connecting to the datastore: " +
          exception.getMessage(), exception);

    } catch (IOException exception) {
      throw new DBException("I/O error connecting to the datastore: " +
          exception.getMessage(), exception);
    }

    logger.info("Datastore client instance created: " +
        datastore.toString());
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
          HashMap<String, ByteIterator> result) {
    LookupRequest.Builder lookupRequest = LookupRequest.newBuilder();
    lookupRequest.addKeys(buildPrimaryKey(table, key));
    lookupRequest.getReadOptionsBuilder().setReadConsistency(
        this.readConsistency);
    // Note above, datastore lookupRequest always reads the entire entity, it
    // does not support reading a subset of "fields" (properties) of an entity.

    logger.debug("Built lookup request as: " + lookupRequest.toString());

    LookupResponse response = null;
    try {
      response = datastore.lookup(lookupRequest.build());

    } catch (DatastoreException exception) {
      logger.error(
          String.format("Datastore Exception when reading (%s): %s %s",
              exception.getMessage(),
              exception.getMethodName(),
              exception.getCode()));

      // DatastoreException.getCode() returns an HTTP response code which we
      // will bubble up to the user as part of the YCSB Status "name".
      return new Status("ERROR-" + exception.getCode(), exception.getMessage());
    }

    if (response.getFoundCount() == 0) {
      return new Status("ERROR-404", "Not Found, key is: " + key);
    } else if (response.getFoundCount() > 1) {
      // We only asked to lookup for one key, shouldn't have got more than one
      // entity back. Unexpected State.
      return Status.UNEXPECTED_STATE;
    }

    Entity entity = response.getFound(0).getEntity();
    logger.debug("Read entity: " + entity.toString());

    Map<String, Value> properties = entity.getProperties();
    Set<String> propertiesToReturn =
        (fields == null ? properties.keySet() : fields);

    for (String name : propertiesToReturn) {
      if (properties.containsKey(name)) {
        result.put(name, new StringByteIterator(properties.get(name)
            .getStringValue()));
      }
    }

    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    // TODO: Implement Scan as query on primary key.
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(String table, String key,
      HashMap<String, ByteIterator> values) {

    return doSingleItemMutation(table, key, values, MutationType.UPDATE);
  }

  @Override
  public Status insert(String table, String key,
      HashMap<String, ByteIterator> values) {
    // Use Upsert to allow overwrite of existing key instead of failing the
    // load (or run) just because the DB already has the key.
    // This is the same behavior as what other DB does here (such as
    // the DynamoDB client).
    return doSingleItemMutation(table, key, values, MutationType.UPSERT);
  }

  @Override
  public Status delete(String table, String key) {
    return doSingleItemMutation(table, key, null, MutationType.DELETE);
  }

  private Key.Builder buildPrimaryKey(String table, String key) {
    Key.Builder result = Key.newBuilder();

    if (this.entityGroupingMode == EntityGroupingMode.MULTI_ENTITY_PER_GROUP) {
      // All entities are in side the same group when we are in this mode.
      result.addPath(Key.PathElement.newBuilder().setKind(table).
          setName(rootEntityName));
    }

    return result.addPath(Key.PathElement.newBuilder().setKind(table)
        .setName(key));
  }

  private Status doSingleItemMutation(String table, String key,
      @Nullable HashMap<String, ByteIterator> values,
      MutationType mutationType) {
    // First build the key.
    Key.Builder datastoreKey = buildPrimaryKey(table, key);

    // Build a commit request in non-transactional mode.
    // Single item mutation to google datastore
    // is always atomic and strongly consistent. Transaction is only necessary
    // for multi-item mutation, or Read-modify-write operation.
    CommitRequest.Builder commitRequest = CommitRequest.newBuilder();
    commitRequest.setMode(Mode.NON_TRANSACTIONAL);

    if (mutationType == MutationType.DELETE) {
      commitRequest.addMutationsBuilder().setDelete(datastoreKey);

    } else {
      // If this is not for delete, build the entity.
      Entity.Builder entityBuilder = Entity.newBuilder();
      entityBuilder.setKey(datastoreKey);
      for (Entry<String, ByteIterator> val : values.entrySet()) {
        entityBuilder.getMutableProperties()
            .put(val.getKey(),
                Value.newBuilder()
                .setStringValue(val.getValue().toString()).build());
      }
      Entity entity = entityBuilder.build();
      logger.debug("entity built as: " + entity.toString());

      if (mutationType == MutationType.UPSERT) {
        commitRequest.addMutationsBuilder().setUpsert(entity);
      } else if (mutationType == MutationType.UPDATE){
        commitRequest.addMutationsBuilder().setUpdate(entity);
      } else {
        throw new RuntimeException("Impossible MutationType, code bug.");
      }
    }

    try {
      datastore.commit(commitRequest.build());
      logger.debug("successfully committed.");

    } catch (DatastoreException exception) {
      // Catch all Datastore rpc errors.
      // Log the exception, the name of the method called and the error code.
      logger.error(
          String.format("Datastore Exception when committing (%s): %s %s",
              exception.getMessage(),
              exception.getMethodName(),
              exception.getCode()));

      // DatastoreException.getCode() returns an HTTP response code which we
      // will bubble up to the user as part of the YCSB Status "name".
      return new Status("ERROR-" + exception.getCode(), exception.getMessage());
    }

    return Status.OK;
  }
}
