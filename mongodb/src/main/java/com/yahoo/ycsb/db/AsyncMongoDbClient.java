/*
 * Copyright (c) 2014, Yahoo!, Inc. All rights reserved.
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

import static com.allanbank.mongodb.builder.QueryBuilder.where;

import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.LockType;
import com.allanbank.mongodb.MongoClient;
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoDatabase;
import com.allanbank.mongodb.MongoDbUri;
import com.allanbank.mongodb.MongoFactory;
import com.allanbank.mongodb.MongoIterator;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.BinaryElement;
import com.allanbank.mongodb.builder.BatchedWrite;
import com.allanbank.mongodb.builder.BatchedWriteMode;
import com.allanbank.mongodb.builder.Find;
import com.allanbank.mongodb.builder.Sort;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * MongoDB asynchronous client for YCSB framework using the <a
 * href="http://www.allanbank.com/mongodb-async-driver/">Asynchronous Java
 * Driver</a>
 * <p>
 * See the <code>README.md</code> for configuration information.
 * </p>
 *
 * @author rjm
 * @see <a href="http://www.allanbank.com/mongodb-async-driver/">Asynchronous
 *      Java Driver</a>
 */
public class AsyncMongoDbClient extends DB {

  /** Used to include a field in a response. */
  protected static final int INCLUDE = 1;

  /** The database to use. */
  private static String databaseName;

  /** Thread local document builder. */
  private static final ThreadLocal<DocumentBuilder> DOCUMENT_BUILDER =
      new ThreadLocal<DocumentBuilder>() {
        @Override
        protected DocumentBuilder initialValue() {
          return BuilderFactory.start();
        }
      };

  /** The write concern for the requests. */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  /** The connection to MongoDB. */
  private static MongoClient mongoClient;

  /** The write concern for the requests. */
  private static Durability writeConcern;

  /** Which servers to use for reads. */
  private static ReadPreference readPreference;

  /** The database to MongoDB. */
  private MongoDatabase database;

  /** The batch size to use for inserts. */
  private static int batchSize;
  
  /** If true then use updates with the upsert option for inserts. */
  private static boolean useUpsert;

  /** The bulk inserts pending for the thread. */
  private final BatchedWrite.Builder batchedWrite = BatchedWrite.builder()
      .mode(BatchedWriteMode.REORDERED);

  /** The number of writes in the batchedWrite. */
  private int batchedWriteCount = 0;

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
        System.err.println("Could not close MongoDB connection pool: "
            + e1.toString());
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
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error. See this class's
   *         description for a discussion of error codes.
   */
  @Override
  public final Status delete(final String table, final String key) {
    try {
      final MongoCollection collection = database.getCollection(table);
      final Document q = BuilderFactory.start().add("_id", key).build();
      final long res = collection.delete(q, writeConcern);
      if (res == 0) {
        System.err.println("Nothing deleted for key " + key);
        return Status.NOT_FOUND;
      }
      return Status.OK;
    } catch (final Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public final void init() throws DBException {
    final int count = INIT_COUNT.incrementAndGet();

    synchronized (AsyncMongoDbClient.class) {
      final Properties props = getProperties();

      if (mongoClient != null) {
        database = mongoClient.getDatabase(databaseName);

        // If there are more threads (count) than connections then the
        // Low latency spin lock is not really needed as we will keep
        // the connections occupied.
        if (count > mongoClient.getConfig().getMaxConnectionCount()) {
          mongoClient.getConfig().setLockType(LockType.MUTEX);
        }

        return;
      }

      // Set insert batchsize, default 1 - to be YCSB-original equivalent
      batchSize = Integer.parseInt(props.getProperty("mongodb.batchsize", "1"));
      
      // Set is inserts are done as upserts. Defaults to false.
      useUpsert = Boolean.parseBoolean(
          props.getProperty("mongodb.upsert", "false"));
      
      // Just use the standard connection format URL
      // http://docs.mongodb.org/manual/reference/connection-string/
      // to configure the client.
      String url =
          props
              .getProperty("mongodb.url", "mongodb://localhost:27017/ycsb?w=1");
      if (!url.startsWith("mongodb://")) {
        System.err.println("ERROR: Invalid URL: '" + url
            + "'. Must be of the form "
            + "'mongodb://<host1>:<port1>,<host2>:<port2>/database?"
            + "options'. See "
            + "http://docs.mongodb.org/manual/reference/connection-string/.");
        System.exit(1);
      }

      MongoDbUri uri = new MongoDbUri(url);

      try {
        databaseName = uri.getDatabase();
        if ((databaseName == null) || databaseName.isEmpty()) {
          // Default database is "ycsb" if database is not
          // specified in URL
          databaseName = "ycsb";
        }

        mongoClient = MongoFactory.createClient(uri);

        MongoClientConfiguration config = mongoClient.getConfig();
        if (!url.toLowerCase().contains("locktype=")) {
          config.setLockType(LockType.LOW_LATENCY_SPIN); // assumed...
        }

        readPreference = config.getDefaultReadPreference();
        writeConcern = config.getDefaultDurability();

        database = mongoClient.getDatabase(databaseName);

        System.out.println("mongo connection created with " + url);
      } catch (final Exception e1) {
        System.err
            .println("Could not initialize MongoDB connection pool for Loader: "
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
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to insert.
   * @param values
   *          A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   *         class's description for a discussion of error codes.
   */
  @Override
  public final Status insert(final String table, final String key,
      final HashMap<String, ByteIterator> values) {
    try {
      final MongoCollection collection = database.getCollection(table);
      final DocumentBuilder toInsert =
          DOCUMENT_BUILDER.get().reset().add("_id", key);
      final Document query = toInsert.build();
      for (final Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        toInsert.add(entry.getKey(), entry.getValue().toArray());
      }

      // Do an upsert.
      if (batchSize <= 1) {
        long result;
        if (useUpsert) {
          result = collection.update(query, toInsert,
              /* multi= */false, /* upsert= */true, writeConcern);
        } else {
          // Return is not stable pre-SERVER-4381. No exception is success.
          collection.insert(writeConcern, toInsert);
          result = 1;
        }
        return result == 1 ? Status.OK : Status.NOT_FOUND;
      }

      // Use a bulk insert.
      try {
        if (useUpsert) {
          batchedWrite.update(query, toInsert, /* multi= */false, 
              /* upsert= */true);
        } else {
          batchedWrite.insert(toInsert);
        }
        batchedWriteCount += 1;

        if (batchedWriteCount < batchSize) {
          return Status.BATCHED_OK;
        }

        long count = collection.write(batchedWrite);
        if (count == batchedWriteCount) {
          batchedWrite.reset().mode(BatchedWriteMode.REORDERED);
          batchedWriteCount = 0;
          return Status.OK;
        }

        System.err.println("Number of inserted documents doesn't match the "
            + "number sent, " + count + " inserted, sent " + batchedWriteCount);
        batchedWrite.reset().mode(BatchedWriteMode.REORDERED);
        batchedWriteCount = 0;
        return Status.ERROR;
      } catch (Exception e) {
        System.err.println("Exception while trying bulk insert with "
            + batchedWriteCount);
        e.printStackTrace();
        return Status.ERROR;
      }
    } catch (final Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   * 
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to read.
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error or "not found".
   */
  @Override
  public final Status read(final String table, final String key,
      final Set<String> fields, final HashMap<String, ByteIterator> result) {
    try {
      final MongoCollection collection = database.getCollection(table);
      final DocumentBuilder query =
          DOCUMENT_BUILDER.get().reset().add("_id", key);

      Document queryResult = null;
      if (fields != null) {
        final DocumentBuilder fieldsToReturn = BuilderFactory.start();
        final Iterator<String> iter = fields.iterator();
        while (iter.hasNext()) {
          fieldsToReturn.add(iter.next(), 1);
        }

        final Find.Builder fb = new Find.Builder(query);
        fb.projection(fieldsToReturn);
        fb.setLimit(1);
        fb.setBatchSize(1);
        fb.readPreference(readPreference);

        final MongoIterator<Document> ci = collection.find(fb.build());
        if (ci.hasNext()) {
          queryResult = ci.next();
          ci.close();
        }
      } else {
        queryResult = collection.findOne(query);
      }

      if (queryResult != null) {
        fillMap(result, queryResult);
      }
      return queryResult != null ? Status.OK : Status.NOT_FOUND;
    } catch (final Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }

  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   * 
   * @param table
   *          The name of the table
   * @param startkey
   *          The record key of the first record to read.
   * @param recordcount
   *          The number of records to read
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A Vector of HashMaps, where each HashMap is a set field/value
   *          pairs for one record
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   *         class's description for a discussion of error codes.
   */
  @Override
  public final Status scan(final String table, final String startkey,
      final int recordcount, final Set<String> fields,
      final Vector<HashMap<String, ByteIterator>> result) {
    try {
      final MongoCollection collection = database.getCollection(table);

      final Find.Builder find =
          Find.builder().query(where("_id").greaterThanOrEqualTo(startkey))
              .limit(recordcount).batchSize(recordcount).sort(Sort.asc("_id"))
              .readPreference(readPreference);

      if (fields != null) {
        final DocumentBuilder fieldsDoc = BuilderFactory.start();
        for (final String field : fields) {
          fieldsDoc.add(field, INCLUDE);
        }

        find.projection(fieldsDoc);
      }

      result.ensureCapacity(recordcount);

      final MongoIterator<Document> cursor = collection.find(find);
      if (!cursor.hasNext()) {
        System.err.println("Nothing found in scan for key " + startkey);
        return Status.NOT_FOUND;
      }
      while (cursor.hasNext()) {
        // toMap() returns a Map but result.add() expects a
        // Map<String,String>. Hence, the suppress warnings.
        final Document doc = cursor.next();
        final HashMap<String, ByteIterator> docAsMap =
            new HashMap<String, ByteIterator>();

        fillMap(docAsMap, doc);

        result.add(docAsMap);
      }

      return Status.OK;
    } catch (final Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   * 
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to write.
   * @param values
   *          A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   *         class's description for a discussion of error codes.
   */
  @Override
  public final Status update(final String table, final String key,
      final HashMap<String, ByteIterator> values) {
    try {
      final MongoCollection collection = database.getCollection(table);
      final DocumentBuilder query = BuilderFactory.start().add("_id", key);
      final DocumentBuilder update = BuilderFactory.start();
      final DocumentBuilder fieldsToSet = update.push("$set");

      for (final Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        fieldsToSet.add(entry.getKey(), entry.getValue().toArray());
      }
      final long res =
          collection.update(query, update, false, false, writeConcern);
      return writeConcern == Durability.NONE || res == 1 ? Status.OK : Status.NOT_FOUND;
    } catch (final Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }

  /**
   * Fills the map with the ByteIterators from the document.
   * 
   * @param result
   *          The map to fill.
   * @param queryResult
   *          The document to fill from.
   */
  protected final void fillMap(final HashMap<String, ByteIterator> result,
      final Document queryResult) {
    for (final Element be : queryResult) {
      if (be.getType() == ElementType.BINARY) {
        result.put(be.getName(),
            new BinaryByteArrayIterator((BinaryElement) be));
      }
    }
  }

  /**
   * BinaryByteArrayIterator provides an adapter from a {@link BinaryElement} to
   * a {@link ByteIterator}.
   */
  private static final class BinaryByteArrayIterator extends ByteIterator {

    /** The binary data. */
    private final BinaryElement binaryElement;

    /** The current offset into the binary element. */
    private int offset;

    /**
     * Creates a new BinaryByteArrayIterator.
     * 
     * @param element
     *          The {@link BinaryElement} to iterate over.
     */
    public BinaryByteArrayIterator(final BinaryElement element) {
      this.binaryElement = element;
      this.offset = 0;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the number of bytes remaining in the iterator.
     * </p>
     */
    @Override
    public long bytesLeft() {
      return Math.max(0, binaryElement.length() - offset);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return true if there is more data in the
     * {@link BinaryElement}.
     * </p>
     */
    @Override
    public boolean hasNext() {
      return (offset < binaryElement.length());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the next value and advance the iterator.
     * </p>
     */
    @Override
    public byte nextByte() {
      final byte value = binaryElement.get(offset);
      offset += 1;

      return value;
    }
  }
}
