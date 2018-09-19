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


import com.google.cloud.firestore.*;
import com.google.api.core.ApiFuture;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;


import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

/**
 * Google Cloud Firestore Client for YCSB.
 */

public class GoogleFirestoreClient extends DB {
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

  private static Logger logger =
      Logger.getLogger(GoogleFirestoreClient.class);


  private CollectionReference firestore = null;

  /**
   * Initialize any state for this DB. Called once per DB instance; there is
   * one DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    String debug = getProperties().getProperty("googlefirestore.debug", null);
    if (null != debug && "true".equalsIgnoreCase(debug)) {
      logger.setLevel(Level.DEBUG);
    }


    // We need the following essential properties to initialize firestore:
    //
    // - ProjectId

    String projectId = getProperties().getProperty(
        "googlefirestore.projectId", null);
    if (projectId == null) {
      throw new DBException(
          "Required property \"projectId\" missing.");
    }

    String collectionId = getProperties().getProperty(
        "googlefirestore.collectionId", null);
    if (collectionId == null) {
      throw new DBException(
          "Required property \"collectionId\" missing.");
    }


    // Below are properties related to benchmarking.


    try {
      // Setup the connection to Google Cloud Firestore with the credentials
      // obtained from the configure.
      FirestoreOptions.Builder firestoreOptions = FirestoreOptions.getDefaultInstance().toBuilder();
      if (projectId != null) {
        firestore = firestoreOptions
            .setProjectId(projectId)
            .build()
            .getService()
            .collection(collectionId);
      }

    } catch (Exception exception) {
      throw new DBException("I/O error connecting to the firestore: " +
          exception.getMessage(), exception);
    }

    logger.info("Firestore client instance created: " +
        firestore.toString());
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {


    DocumentSnapshot response;
    try {
      response = firestore.document(key).get().get();

    } catch (Exception exception) {
      logger.error(
          String.format("Firestore Exception when reading (%s) ",
              exception.getMessage(), exception)
      );

      return Status.ERROR;
    }

    if (response == null) {
      return new Status("ERROR-404", "Not Found, key is: " + key);
    }

    logger.debug("Read entity: " + response.getData().toString());

    Map<String, Object> properties = response.getData();
    Set<String> propertiesToReturn =
        (fields == null ? properties.keySet() : fields);

    for (String name : propertiesToReturn) {
      if (properties.containsKey(name)) {
        result.put(name, new StringByteIterator(properties.get(name).toString()));
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
                       Map<String, ByteIterator> values) {

    return doSingleItemMutation(table, key, values, MutationType.UPDATE);
  }

  @Override
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {
    return doSingleItemMutation(table, key, values, MutationType.UPSERT);
  }

  @Override
  public Status delete(String table, String key) {
    return doSingleItemMutation(table, key, null, MutationType.DELETE);
  }


  private Status doSingleItemMutation(String table, String key,
                                      @Nullable Map<String, ByteIterator> values,
                                      MutationType mutationType) {

    // Build a commit request in non-transactional mode.
    // Single item mutation to google firestore
    // is always atomic and strongly consistent. Transaction is only necessary
    // for multi-item mutation, or Read-modify-write operation.

    Map<String, String> update = null;
    if (values != null) {
      update = values.entrySet().stream()
          .collect(
              Collectors.toMap(
                  entry -> entry.getKey(), entry -> entry.getValue().toString()
              )
          );
    }

    ApiFuture<WriteResult> writeResult = null;
    if (mutationType == MutationType.DELETE) {
      writeResult = firestore.document(key).delete();
    } else if (mutationType == MutationType.UPSERT) {
      writeResult = firestore.document(key)
          .set(update, SetOptions.merge());
    } else if (mutationType == MutationType.UPDATE) {
      writeResult = firestore.document(key)
          .set(update, SetOptions.merge());
    } else {
      throw new RuntimeException("Impossible MutationType, code bug.");
    }


    try {
      logger.debug("successfully committed at " + writeResult.get().getUpdateTime());

    } catch (Exception exception) {
      // Catch all Firestore rpc errors.
      // Log the exception.
      logger.error(
          String.format("Firestore Exception when committing (%s)",
              exception.getMessage()), exception);

      // will bubble up to the user as part of the YCSB Status "name".
      return Status.ERROR;
    }

    return Status.OK;
  }
}
