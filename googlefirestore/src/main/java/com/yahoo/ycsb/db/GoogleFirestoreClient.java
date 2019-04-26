/*
 * Copyright (c) 2019 YCSB contributors. All rights reserved.
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

import com.google.api.core.ApiFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.cloud.firestore.SetOptions;
import com.google.cloud.firestore.WriteResult;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ExecutionException;

/**
 * YCSB Client for Google's Cloud Firestore.
 */
public class GoogleFirestoreClient extends DB {
  /**
   * The names of properties which can be specified in the config files and flags.
   */
  public static final class GoogleFirestoreProperties {
    private GoogleFirestoreProperties() {}

    /**
     * The Cloud Firestore project ID to use when running the YCSB benchmark.
     */
    static final String PROJECT = "googlefirestore.projectId";
    static final String PRIVKEYFILE = "googlefirestore.serviceAccountKey";
    static final String DEBUG = "googlefirestore.debug";
  }

  private static final Logger LOGGER = Logger.getLogger(GoogleFirestoreClient.class);

  private static Firestore db;

  @Override
  public void init() throws DBException {
    if (db != null) {
      return;
    }
    Properties properties = getProperties();
    String debug = properties.getProperty(GoogleFirestoreProperties.DEBUG);
    if (null != debug && "true".equalsIgnoreCase(debug)) {
      LOGGER.setLevel(Level.DEBUG);
    }
    String projectId = properties.getProperty(GoogleFirestoreProperties.PROJECT);
    if (projectId == null) {
      throw new DBException("Must provide project ID.");
    }
    String privateKeyFile = properties.getProperty(GoogleFirestoreProperties.PRIVKEYFILE);
    if (privateKeyFile == null) {
      throw new DBException("Must provide full path to private key file.");
    }
    try {
      GoogleCredentials gCreds = GoogleCredentials.fromStream(new FileInputStream(privateKeyFile));
      FirestoreOptions fsOptions = FirestoreOptions.newBuilder().setCredentials(gCreds).build();
      db = fsOptions.getService();
    } catch (FileNotFoundException e) {
      LOGGER.error("Can't find key.", e);
    } catch (IOException e) {
      LOGGER.error("No file to import.", e);
    }

    LOGGER.info("Created Firestore client for project: "+ projectId);
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    DocumentReference docRef = toReference(table, key);

    try {
      DocumentSnapshot docSs = docRef.get().get();
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("read: Collection: " + table + " document: " + key);
      }
      // If document doesn't exist, will cause NPE.
      if (docSs.exists()) {
        parseFields(fields, docSs, result);
        return Status.OK;
      } else {
        LOGGER.error("read: Referenced document is missing. : " + docSs.getReference().toString());
        return Status.ERROR;
      }

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.error("Interrupted during read().", e);
      return Status.ERROR;
    } catch (ExecutionException e) {
      LOGGER.error("Error during read().", e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    Query query = db.collection(table)
        .orderBy("__name__")
        .startAt(startkey)
        .limit(recordcount);

    try {
      QuerySnapshot querySs = query.get().get();
      for (DocumentSnapshot docSs : querySs.getDocuments()) {
        HashMap<String, ByteIterator> scanres = new HashMap<>();
        parseFields(fields, docSs, scanres);
        result.add(scanres);
      }
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("scan: " + startkey + " : " + recordcount);
      }
      return Status.OK;

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.error("Interrupted during scan().", e);
      return Status.ERROR;
    } catch (ExecutionException e) {
      LOGGER.error("Error during scan().", e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    DocumentReference docRef = toReference(table, key);
    Map<String, Object> data = toData(values);

    try {
      docRef.set(data, SetOptions.merge()).get();
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("update: Document: " + key + " : " + data.toString());
      }
      return Status.OK;

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.error("Interrupted during update().", e);
      return Status.ERROR;
    } catch (ExecutionException e) {
      LOGGER.error("Error during update().", e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    DocumentReference docRef = toReference(table, key);
    Map<String, Object> data = toData(values);

    try {
      ApiFuture<WriteResult> writeResult = docRef.set(data, SetOptions.merge());
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("insert: Update time: " + writeResult.get().getUpdateTime());
      }
      return Status.OK;

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.error("Interrupted during insert().", e);
      return Status.ERROR;
    } catch (ExecutionException e) {
      LOGGER.error("Error during insert().", e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    DocumentReference docRef = toReference(table, key);

    try {
      docRef.delete().get();
      if (LOGGER.isDebugEnabled()){
        LOGGER.debug("delete: Document: " + docRef.toString());
      }
      return Status.OK;

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.error("Interrupted during delete().", e);
      return Status.ERROR;
    } catch (ExecutionException e) {
      LOGGER.error("Error during delete().", e);
      return Status.ERROR;
    }
  }

  private void parseFields(
      Set<String> fields, DocumentSnapshot docSs, Map<String, ByteIterator> result) {
    Set<String> docFields = fields == null ? docSs.getData().keySet() : fields;
    for (String field : docFields) {
      String value = docSs.getString(field);
      result.put(field, new StringByteIterator(value));
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("parse: field: " + field + " value: " + value);
      }
    }
  }

  private DocumentReference toReference(String table, String key) {
    return db.collection(table).document(key);
  }

  private Map<String, Object> toData(Map<String, ByteIterator> values) {
    Map<String, Object> data = new HashMap<>(values.size());
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      data.put(entry.getKey(), entry.getValue().toString());
    }
    return data;
  }
}