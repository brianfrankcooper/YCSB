
package com.yahoo.ycsb.db;

import com.yahoo.ycsb.*;

import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;
import java.util.List;

public class DocumentDBClient extends DB {
  private static String host;
  private static String masterKey;
  private static String databaseId;
  private static Database database;
  private static DocumentClient documentClient;
  private static DocumentCollection collection;

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
    documentClient = new DocumentClient(host, masterKey,
            ConnectionPolicy.GetDefault(), ConsistencyLevel.Session);
    // Initialize test database and collection.
    getDatabase();
  }

  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    Document record = getDocumentById(table, key);

    if (record != null) {
      Set<String> fieldsToReturn = (fields == null ? record.getHashMap().keySet() : fields);

      for (String field : fieldsToReturn) {
        result.put(field, new StringByteIterator(record.getString(field)));  
      }
      return Status.OK;
    }
    // Unable to find the specidifed document.
    return Status.ERROR;
  }

  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
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
      e.printStackTrace();
      return Status.ERROR;
    }

    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    Document record = new Document();

    for (Entry<String, ByteIterator> val : values.entrySet()) {
      record.set(val.getKey(), val.getValue().toString());
    }

    try {
      documentClient.createDocument(collection.getSelfLink(), record, null, false);
    } catch (DocumentClientException e) {
      e.printStackTrace();
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
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    // TODO: Implement Scan as query on primary key.
    return Status.NOT_IMPLEMENTED;
  }

  private Database getDatabase() {
    if (database == null) {
        // Get the database if it exists
        List<Database> databaseList = documentClient
                .queryDatabases(
                        "SELECT * FROM root r WHERE r.id='" + DATABASE_ID
                                + "'", null).getQueryIterable().toList();

        if (databaseList.size() > 0) {
            // Cache the database object so we won't have to query for it
            // later to retrieve the selfLink.
            database = databaseList.get(0);
        } else {
            // Create the database if it doesn't exist.
            try {
                Database databaseDefinition = new Database();
                databaseDefinition.setId(DATABASE_ID);

                database = documentClient.createDatabase(
                        databaseDefinition, null).getResource();
            } catch (DocumentClientException e) {
                // TODO: Something has gone terribly wrong - the app wasn't
                // able to query or create the collection.
                // Verify your connection, endpoint, and key.
                e.printStackTrace();
            }
        }
    }

    return database;
  }

  private DocumentCollection getCollection(String collectionId) {
    if (collection == null) {
      // Get the collection if it exists.
      List<DocumentCollection> collectionList = documentClient
              .queryCollections(
                      getDatabase().getSelfLink(),
                      "SELECT * FROM root r WHERE r.id='" + collectionId
                              + "'", null).getQueryIterable().toList();

      if (collectionList.size() > 0) {
        // Cache the collection object so we won't have to query for it
        // later to retrieve the selfLink.
        collection = collectionList.get(0);
      } else {
        // Create the collection if it doesn't exist.
        try {
          DocumentCollection collectionDefinition = new DocumentCollection();
          collectionDefinition.setId(collectionId);

          collection = documentClient.createCollection(
                  getDatabase().getSelfLink(),
                  collectionDefinition, null).getResource();
        } catch (DocumentClientException e) {
          // TODO: Something has gone terribly wrong - the app wasn't
          // able to query or create the collection.
          // Verify your connection, endpoint, and key.
          e.printStackTrace();
        }
      }
    }

    return collection;
  }

  private Document getDocumentById(String collectionId, String id) {
    // Retrieve the document using the DocumentClient.
    List<Document> documentList = documentClient
            .queryDocuments(getCollection(collectionId).getSelfLink(),
                    "SELECT * FROM root r WHERE r.id='" + id + "'", null)
            .getQueryIterable().toList();

    if (documentList.size() > 0) {
      return documentList.get(0);
    } else {
      return null;
    }
  }
}