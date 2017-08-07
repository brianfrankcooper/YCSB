/*
 * Copyright 2016 YCSB Contributors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 *
 * This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under
 * the License.
 */
package com.yahoo.ycsb.db.azuredocumentdb;

import com.yahoo.ycsb.*;

import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.FeedOptions;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;
import java.util.List;

/**
 * Azure DocumentDB client binding.
 */
public class AzureDocumentDBClient extends DB {
  private static String host;
  private static String masterKey;
  private static String databaseId;
  private static Database database;
  private static DocumentClient documentClient;
  private static DocumentCollection collection;
  private static FeedOptions feedOptions;

  @Override
  public void init() throws DBException {
    host = getProperties().getProperty("documentdb.host", null);
    masterKey = getProperties().getProperty("documentdb.masterKey", null);

    if (host == null) {
      System.err.println("ERROR: 'documentdb.host' must be set!");
      System.exit(1);
    }

    if (masterKey == null) {
      System.err.println("ERROR: 'documentdb.masterKey' must be set!");
      System.exit(1);
    }

    databaseId = getProperties().getProperty("documentdb.databaseId", "ycsb");
    String collectionId =
        getProperties().getProperty("documentdb.collectionId", "usertable");
    documentClient =
        new DocumentClient(host, masterKey, ConnectionPolicy.GetDefault(),
                           ConsistencyLevel.Session);
    try {
      // Initialize test database and collection.
      collection = getCollection(collectionId);
    } catch (DocumentClientException e) {
      throw new DBException("Initialze collection failed", e);
    }

    feedOptions = new FeedOptions();
    feedOptions.setEmitVerboseTracesInQuery(false);
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    Document record = getDocumentById(table, key);

    if (record != null) {
      Set<String> fieldsToReturn =
          (fields == null ? record.getHashMap().keySet() : fields);

      for (String field : fieldsToReturn) {
        if (field.startsWith("_")) {
          continue;
        }
        result.put(field, new StringByteIterator(record.getString(field)));
      }
      return Status.OK;
    }
    // Unable to find the specidifed document.
    return Status.ERROR;
  }

  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    Document record = getDocumentById(table, key);

    if (record == null) {
      return Status.ERROR;
    }

    // Update each field.
    for (Entry<String, ByteIterator> val : values.entrySet()) {
      record.set(val.getKey(), val.getValue().toString());
    }

    // Replace the document.
    try {
      documentClient.replaceDocument(record, null);
    } catch (DocumentClientException e) {
      e.printStackTrace(System.err);
      return Status.ERROR;
    }

    return Status.OK;
  }

  @Override
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {
    Document record = new Document();

    record.set("id", key);

    for (Entry<String, ByteIterator> val : values.entrySet()) {
      record.set(val.getKey(), val.getValue().toString());
    }

    try {
      documentClient.createDocument(collection.getSelfLink(), record, null,
                                    false);
    } catch (DocumentClientException e) {
      e.printStackTrace(System.err);
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    Document record = getDocumentById(table, key);

    try {
      // Delete the document by self link.
      documentClient.deleteDocument(record.getSelfLink(), null);
    } catch (DocumentClientException e) {
      e.printStackTrace();
      return Status.ERROR;
    }

    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    // TODO: Implement Scan as query on primary key.
    return Status.NOT_IMPLEMENTED;
  }

  private Database getDatabase() {
    if (database == null) {
      // Get the database if it exists
      List<Database> databaseList =
          documentClient
              .queryDatabases(
                  "SELECT * FROM root r WHERE r.id='" + databaseId + "'", null)
              .getQueryIterable()
              .toList();

      if (databaseList.size() > 0) {
        // Cache the database object so we won't have to query for it
        // later to retrieve the selfLink.
        database = databaseList.get(0);
      } else {
        // Create the database if it doesn't exist.
        try {
          Database databaseDefinition = new Database();
          databaseDefinition.setId(databaseId);

          database = documentClient.createDatabase(databaseDefinition, null)
                         .getResource();
        } catch (DocumentClientException e) {
          // TODO: Something has gone terribly wrong - the app wasn't
          // able to query or create the collection.
          // Verify your connection, endpoint, and key.
          e.printStackTrace(System.err);
        }
      }
    }

    return database;
  }

  private DocumentCollection getCollection(String collectionId)
      throws DocumentClientException {
    if (collection == null) {
      // Get the collection if it exists.
      List<DocumentCollection> collectionList =
          documentClient
              .queryCollections(getDatabase().getSelfLink(),
                                "SELECT * FROM root r WHERE r.id='" +
                                    collectionId + "'",
                                null)
              .getQueryIterable()
              .toList();

      if (collectionList.size() > 0) {
        // Cache the collection object so we won't have to query for it
        // later to retrieve the selfLink.
        collection = collectionList.get(0);
      } else {
        // Create the collection if it doesn't exist.
        try {
          DocumentCollection collectionDefinition = new DocumentCollection();
          collectionDefinition.setId(collectionId);

          collection = documentClient
                           .createCollection(getDatabase().getSelfLink(),
                                             collectionDefinition, null)
                           .getResource();
        } catch (DocumentClientException e) {
          // TODO: Something has gone terribly wrong - the app wasn't
          // able to query or create the collection.
          // Verify your connection, endpoint, and key.
          e.printStackTrace(System.err);
          throw e;
        }
      }
    }

    return collection;
  }

  private Document getDocumentById(String collectionId, String id) {
    if (collection == null) {
      return null;
    }
    // Retrieve the document using the DocumentClient.
    List<Document> documentList =
        documentClient
            .queryDocuments(collection.getSelfLink(),
                            "SELECT * FROM root r WHERE r.id='" + id + "'",
                            feedOptions)
            .getQueryIterable()
            .toList();

    if (documentList.size() > 0) {
      return documentList.get(0);
    }
    return null;
  }
}
