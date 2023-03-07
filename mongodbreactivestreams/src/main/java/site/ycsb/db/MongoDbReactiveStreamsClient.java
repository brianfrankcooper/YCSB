/*
 * Copyright (c) 2022, Yahoo!, Inc. All rights reserved.
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
package site.ycsb.db;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.Binary;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * MongoDB Reactive Streams client for YCSB framework.
 */
public class MongoDbReactiveStreamsClient extends DB {
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbReactiveStreamsClient.class);
  private static final String DEFAULT_DATABASE_NAME = "ycsb";
  private static final boolean DEFAULT_USE_UPSERT = false;
  private static final int DEFAULT_BATCH_SIZE = 1;
  /**
   * Used to include a field in a response.
   */
  protected static final int INCLUDE = 1;

  /**
   * The database to use.
   */
  private static String databaseName;

  /**
   * The write concern for the requests.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  /**
   * The connection to MongoDB.
   */
  private static MongoClient mongoClient;

  /**
   * The database to MongoDB.
   */
  private static MongoDatabase database;

  /**
   * The batch size to use for inserts.
   */
  private static int batchSize;

  /**
   * If true then use updates with the upsert option for inserts.
   */
  private static boolean useUpsert;

  /**
   * The default read preference for the test.
   */
  private static ReadPreference readPreference;

  /**
   * The default write concern for the test.
   */
  private static WriteConcern writeConcern;

  //  /** The bulk inserts pending for the thread. */
  private final List<Document> bulkInserts = new ArrayList<Document>();

  private static final ReplaceOptions REPLACE_WITH_UPSERT = new ReplaceOptions()
      .upsert(true);

  private static final InsertManyOptions INSERT_UNORDERED =
      new InsertManyOptions().ordered(false);

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  @Override
  public final void cleanup() throws DBException {
    if (INIT_COUNT.decrementAndGet() == 0) {
      try {
        mongoClient.close();
      } catch (final Exception e1) {
        LOGGER.error("Could not close MongoDB connection pool: " + e1.toString());
        e1.printStackTrace();
        return;
      } finally {
        mongoClient = null;
        database = null;
      }
    }
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key   The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error. See this class's
   * description for a discussion of error codes.
   */
  @Override
  public final Status delete(final String table, final String key) {
    try {
      final MongoCollection collection = database.getCollection(table);
      final Document q = new Document("_id", key); //Builders.start().add("_id", key).build();
      OperationSubscriber<DeleteResult> deleteSubscriber = new OperationSubscriber();
      collection.withWriteConcern(writeConcern).deleteOne(q).subscribe(deleteSubscriber);
      if (deleteSubscriber.first() == null || deleteSubscriber.first().getDeletedCount() <= 0) {
        LOGGER.error("Nothing deleted for key " + key);
        return Status.NOT_FOUND;
      }
      return Status.OK;
    } catch (final Exception e) {
      LOGGER.error(e.toString());
      return Status.ERROR;
    }
  }

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public final void init() throws DBException {
    INIT_COUNT.incrementAndGet();

    synchronized (MongoDbReactiveStreamsClient.class) {
      if (mongoClient != null) {
        return;
      }

      // Set insert batchsize, default 1 - to be YCSB-original equivalent
      batchSize = this.getIntProperty("mongodb.batchsize", DEFAULT_BATCH_SIZE);

      // Set is inserts are done as upserts. Defaults to false.
      useUpsert = this.getBooleanProperty("mongodb.upsert", DEFAULT_USE_UPSERT);

      // Just use the standard connection format URL
      // http://docs.mongodb.org/manual/reference/connection-string/
      // to configure the client.
      String url = this.getStringProperty("mongodb.url", "mongodb://localhost:27017/ycsb?w=1");
      url = OptionsSupport.updateUrl(url, getProperties());

      if (!url.startsWith("mongodb://")) {
        LOGGER.error("ERROR: Invalid URL: '" + url
            + "'. Must be of the form "
            + "'mongodb://<host1>:<port1>,<host2>:<port2>/database?"
            + "options'. See "
            + "http://docs.mongodb.org/manual/reference/connection-string/.");
        System.exit(1);
      }

      ConnectionString connectionString = new ConnectionString(url);
      try {
        databaseName = connectionString.getDatabase();
        if ((databaseName == null) || databaseName.isEmpty()) {
          // Default database is "ycsb" if database is not
          // specified in URL
          databaseName = this.getStringProperty("mongodb.databaseName", DEFAULT_DATABASE_NAME);
        }
        MongoClientSettings settings = MongoClientSettings.builder()
            .applyConnectionString(connectionString)
            .retryWrites(false)
            .build();

        mongoClient = MongoClients.create(settings);

        readPreference = settings.getReadPreference();
        writeConcern = settings.getWriteConcern();

        database = mongoClient.getDatabase(databaseName);
        LOGGER.info("mongo connection created with " + url);
      } catch (final Exception e1) {
        LOGGER.error("Could not initialize MongoDB connection pool for Loader: "
            + e1.toString());
        e1.printStackTrace();
        return;
      }
    }
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   * class's description for a discussion of error codes.
   */
  @Override
  public final Status insert(String table, String key,
                             Map<String, ByteIterator> values) {
    try {
      final MongoCollection collection = database.getCollection(table);
      final Document toInsert = new Document("_id", key);
      for (final Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        toInsert.put(entry.getKey(), entry.getValue().toArray());
      }

      OperationSubscriber insertSubscriber;
      // Do an upsert.
      if (batchSize <= 1) {
        long result;
        if (useUpsert) {
          insertSubscriber = new OperationSubscriber<UpdateResult>();
          collection.replaceOne(new Document("_id", toInsert.get("_id")), toInsert, REPLACE_WITH_UPSERT)
              .subscribe(insertSubscriber);
        } else {
          insertSubscriber = new OperationSubscriber<InsertOneResult>();
          collection.insertOne(toInsert).subscribe(insertSubscriber);
        }
        insertSubscriber.await();
      } else {
        // Use a bulk insert.
        bulkInserts.add(toInsert);
        if (bulkInserts.size() == batchSize) {
          if (useUpsert) {
            List<ReplaceOneModel<Document>> updates =
                new ArrayList<ReplaceOneModel<Document>>(bulkInserts.size());
            for (Document doc : bulkInserts) {
              updates.add(new ReplaceOneModel<Document>(
                  new Document("_id", doc.get("_id")),
                  doc, REPLACE_WITH_UPSERT));
            }
            insertSubscriber = new OperationSubscriber<BulkWriteResult>();
            collection.bulkWrite(updates).subscribe(insertSubscriber);
          } else {
            insertSubscriber = new OperationSubscriber<InsertManyResult>();
            collection.insertMany(bulkInserts, INSERT_UNORDERED).subscribe(insertSubscriber);
          }
          insertSubscriber.await();
          bulkInserts.clear();
        } else {
          return Status.BATCHED_OK;
        }
      }
      return Status.OK;
    } catch (final Exception e) {
      e.printStackTrace();
      return Status.ERROR;
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
   * @return Zero on success, a non-zero error code on error or "not found".
   */
  @Override
  public final Status read(final String table, final String key,
                           final Set<String> fields, final Map<String, ByteIterator> result) {
    try {
      MongoCollection collection = database.getCollection(table);
      OperationSubscriber<Document> readSubscriber = new OperationSubscriber();
      Document query = new Document("_id", key);
      FindPublisher<Document> findPublisher = collection.find(query);
      if (fields != null) {
        Document projection = new Document();
        for (String field : fields) {
          projection.put(field, INCLUDE);
        }
        findPublisher.projection(projection);
      }
      findPublisher.subscribe(readSubscriber);
      Document queryResult = readSubscriber.first();
      if (queryResult != null) {
        fillMap(result, queryResult);
      }
      return queryResult != null ? Status.OK : Status.NOT_FOUND;
    } catch (final Exception e) {
      LOGGER.error(e.toString());
      return Status.ERROR;
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
   * @param result      A Vector of HashMaps, where each HashMap is a set field/value
   *                    pairs for one record
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   * class's description for a discussion of error codes.
   */
  @Override
  public final Status scan(String table, String startkey,
                           int recordcount, Set<String> fields,
                           Vector<HashMap<String, ByteIterator>> result) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);
      Document scanRange = new Document("$gte", startkey);
      Document query = new Document("_id", scanRange);
      Document sort = new Document("_id", INCLUDE);

      FindPublisher<Document> findPublisher = collection.find(query).sort(sort).limit(1);
      if (fields != null) {
        Document projection = new Document();
        for (String fieldName : fields) {
          projection.put(fieldName, INCLUDE);
        }
        findPublisher.projection(projection);
      }

      result.ensureCapacity(recordcount);

      QuerySubscriber querySubscriber = new QuerySubscriber(result);
      findPublisher.subscribe(querySubscriber);
      querySubscriber.await();

      return Status.OK;
    } catch (final Exception e) {
      LOGGER.error((e.toString()));
      return Status.ERROR;
    }
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   * class's description for a discussion of error codes.
   */
  @Override
  public final Status update(String table, String key,
                             Map<String, ByteIterator> values) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);
      Document query = new Document("_id", key);
      Document fieldsToSet = new Document();
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        fieldsToSet.put(entry.getKey(), entry.getValue().toArray());
      }
      Document update = new Document("$set", fieldsToSet);
      OperationSubscriber<UpdateResult> updateSubscriber = new OperationSubscriber();
      collection.updateOne(query, update).subscribe(updateSubscriber);
      UpdateResult result = updateSubscriber.first();
      if (result.wasAcknowledged() && result.getMatchedCount() == 0) {
        LOGGER.error("Nothing updated for key " + key);
        return Status.NOT_FOUND;
      }
      return Status.OK;
    } catch (final Exception e) {
      LOGGER.error(e.toString());
      return Status.ERROR;
    }
  }

  /**
   * Fills the map with the ByteIterators from the document.
   *
   * @param result      The map to fill.
   * @param queryResult The document to fill from.
   */
  protected static final void fillMap(final Map<String, ByteIterator> result,
                                      final Document queryResult) {
    for (Map.Entry<String, Object> entry : queryResult.entrySet()) {
      if (entry.getValue() instanceof Binary) {
        result.put(entry.getKey(),
            new ByteArrayByteIterator(((Binary) entry.getValue()).getData()));
      }
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
   * A Subscriber that stores the publishers results and provides a latch so can block on completion.
   *
   * @param <T> The publishers result type
   */
  public abstract static class ObservableSubscriber<T> implements Subscriber<T> {
    private final List<T> received;
    private final List<RuntimeException> errors;
    private final CountDownLatch latch;
    private volatile Subscription subscription;
    private volatile boolean completed;

    /**
     * Construct an instance.
     */
    public ObservableSubscriber() {
      this.received = new ArrayList<>();
      this.errors = new ArrayList<>();
      this.latch = new CountDownLatch(1);
    }

    @Override
    public void onSubscribe(final Subscription sub) {
      this.subscription = sub;
    }

    @Override
    public void onNext(final T t) {
      received.add(t);
    }

    @Override
    public void onError(final Throwable throwable) {
      if (throwable instanceof RuntimeException) {
        errors.add((RuntimeException) throwable);
      } else {
        errors.add(new RuntimeException("Unexpected exception", throwable));
      }
      onComplete();
    }

    @Override
    public void onComplete() {
      completed = true;
      latch.countDown();
    }

    /**
     * Get received elements.
     *
     * @return the list of received elements.
     */
    public List<T> getReceived() {
      return received;
    }

    /**
     * Get received elements.
     *
     * @return the list of receive elements
     */
    public List<T> get() {
      return await().getReceived();
    }

    /**
     * Get the first received element.
     *
     * @return the first received element
     */
    public T first() {
      List<T> receivedElements = await().getReceived();
      return receivedElements.size() > 0 ? receivedElements.get(0) : null;
    }

    /**
     * Await completion or error.
     *
     * @return this
     */
    public ObservableSubscriber<T> await() {
      return await(300, TimeUnit.SECONDS);
    }

    /**
     * Await completion or error.
     *
     * @param timeout how long to wait
     * @param unit    the time unit
     * @return this
     */
    public ObservableSubscriber<T> await(final long timeout, final TimeUnit unit) {
      subscription.request(Integer.MAX_VALUE);
      try {
        if (!latch.await(timeout, unit)) {
          throw new MongoTimeoutException("Publisher onComplete timed out");
        }
      } catch (InterruptedException e) {
        throw new MongoInterruptedException("Interrupted waiting for observeration", e);
      }
      if (!errors.isEmpty()) {
        throw errors.get(0);
      }
      return this;
    }
  }

  /**
   * A CRUD operation Subscriber.
   *
   * @param <T> The publishers result type
   */
  public static class OperationSubscriber<T> extends ObservableSubscriber<T> {
    @Override
    public void onSubscribe(final Subscription s) {
      super.onSubscribe(s);
      s.request(Integer.MAX_VALUE);
    }
  }

  /**
   * A Subscriber that prints the json version of each document.
   */
  public static class PrintDocumentSubscriber extends OperationSubscriber<Document> {

    @Override
    public void onNext(final Document document) {
      super.onNext(document);
      LOGGER.info(document.toJson());
    }
  }

  /**
   * A Query Subscriber.
   */
  public static class QuerySubscriber extends ObservableSubscriber<Document> {
    private Vector<HashMap<String, ByteIterator>> result;

    public QuerySubscriber(Vector<HashMap<String, ByteIterator>> result) {
      this.result = result;
    }

    @Override
    public void onSubscribe(final Subscription s) {
      super.onSubscribe(s);
      s.request(Integer.MAX_VALUE);
    }

    @Override
    public void onNext(final Document t) {
      LOGGER.info(t.toJson());
      HashMap<String, ByteIterator> resultMap =
          new HashMap<String, ByteIterator>();
      fillMap(resultMap, t);
      result.add(resultMap);
    }
  }
}
