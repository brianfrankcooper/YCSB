/**                                                                                                                                                                                
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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

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
import com.allanbank.mongodb.builder.Find;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

/**
 * MongoDB asynchronous client for YCSB framework.
 * 
 * Properties to set:
 * 
 * mongodb.url=mongodb://localhost:27017 mongodb.database=ycsb
 * mongodb.writeConcern=normal
 * 
 * @author rjm
 */
public class AsyncMongoDbClient extends DB {

    /** The database to use. */
    private static String database;

    /** Thread local document builder. */
    private static final ThreadLocal<DocumentBuilder> DOCUMENT_BUILDER = new ThreadLocal<DocumentBuilder>() {
        @Override
        protected DocumentBuilder initialValue() {
            return BuilderFactory.start();
        }
    };

    /** The write concern for the requests. */
    private static final AtomicInteger initCount = new AtomicInteger(0);

    /** The connection to MongoDB. */
    private static MongoClient mongo;

    /** The write concern for the requests. */
    private static Durability writeConcern;

    /** Which servers to use for reads. */
    private static ReadPreference readPreference;

    /** The database to MongoDB. */
    private MongoDatabase db;

    /**
     * Cleanup any state for this DB. Called once per DB instance; there is one
     * DB instance per client thread.
     */
    @Override
    public final void cleanup() throws DBException {
        if (initCount.decrementAndGet() <= 0) {
            try {
                mongo.close();
            }
            catch (final Exception e1) {
                System.err.println("Could not close MongoDB connection pool: "
                        + e1.toString());
                e1.printStackTrace();
                return;
            }
        }
    }

    /**
     * Delete a record from the database.
     * 
     * @param table
     *            The name of the table
     * @param key
     *            The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error. See this class's
     *         description for a discussion of error codes.
     */
    @Override
    public final int delete(final String table, final String key) {
        try {
            final MongoCollection collection = db.getCollection(table);
            final Document q = BuilderFactory.start().add("_id", key).build();
            final long res = collection.delete(q, writeConcern);
            return res == 1 ? 0 : 1;
        }
        catch (final Exception e) {
            System.err.println(e.toString());
            return 1;
        }
    }

    /**
     * Initialize any state for this DB. Called once per DB instance; there is
     * one DB instance per client thread.
     */
    @Override
    public final void init() throws DBException {
        final int count = initCount.incrementAndGet();

        synchronized (AsyncMongoDbClient.class) {
            if (mongo != null) {
                db = mongo.getDatabase(database);

                // If there are more threads (count) than connections then the
                // Low latency spin lock is not really needed as we will keep
                // the connections occupied.
                if (count > mongo.getConfig().getMaxConnectionCount()) {
                    mongo.getConfig().setLockType(LockType.MUTEX);
                }

                return;
            }

            // Just use the standard connection format URL
            // http://docs.mongodb.org/manual/reference/connection-string/
            final Properties props = getProperties();
            String url = props.getProperty("mongodb.url",
                    "mongodb://localhost:27017/ycsb?w=1");
            if (!url.startsWith("mongodb://")) {
                System.err
                        .println("ERROR: Invalid URL: '"
                                + url
                                + "'. Must be of the form "
                                + "'mongodb://<host1>:<port1>,<host2>:<port2>/database?options'. "
                                + "See http://docs.mongodb.org/manual/reference/connection-string/.");
                System.exit(1);
            }

            MongoDbUri uri = new MongoDbUri(url);

            try {
                database = uri.getDatabase();
                if ((database == null) || database.isEmpty()) {
                    System.err
                            .println("ERROR: Invalid URL: '"
                                    + url
                                    + "'. Must provide a database name with the URI. "
                                    + "'mongodb://<host1>:<port1>,<host2>:<port2>/database");
                    System.exit(1);
                }

                mongo = MongoFactory.createClient(uri);

                MongoClientConfiguration config = mongo.getConfig();
                if (!url.toLowerCase().contains("locktype=")) {
                    config.setLockType(LockType.LOW_LATENCY_SPIN); // assumed...
                }

                readPreference = config.getDefaultReadPreference();
                writeConcern = config.getDefaultDurability();

                db = mongo.getDatabase(database);

                System.out.println("mongo connection created with " + url);
            }
            catch (final Exception e1) {
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
     *            The name of the table
     * @param key
     *            The record key of the record to insert.
     * @param values
     *            A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error. See this class's
     *         description for a discussion of error codes.
     */
    @Override
    public final int insert(final String table, final String key,
            final HashMap<String, ByteIterator> values) {
        try {
            final MongoCollection collection = db.getCollection(table);
            final DocumentBuilder r = DOCUMENT_BUILDER.get().reset()
                    .add("_id", key);
            final Document q = r.build();
            for (final Map.Entry<String, ByteIterator> entry : values
                    .entrySet()) {
                r.add(entry.getKey(), entry.getValue().toArray());
            }
            collection.insert(writeConcern, r);

            collection.update(q, r, /* multi= */false, /* upsert= */true,
                    writeConcern);

            return 0;
        }
        catch (final Exception e) {
            e.printStackTrace();
            return 1;
        }
    }

    /**
     * Read a record from the database. Each field/value pair from the result
     * will be stored in a HashMap.
     * 
     * @param table
     *            The name of the table
     * @param key
     *            The record key of the record to read.
     * @param fields
     *            The list of fields to read, or null for all of them
     * @param result
     *            A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    @Override
    public final int read(final String table, final String key,
            final Set<String> fields, final HashMap<String, ByteIterator> result) {
        try {
            final MongoCollection collection = db.getCollection(table);
            final DocumentBuilder q = BuilderFactory.start().add("_id", key);
            final DocumentBuilder fieldsToReturn = BuilderFactory.start();

            Document queryResult = null;
            if (fields != null) {
                final Iterator<String> iter = fields.iterator();
                while (iter.hasNext()) {
                    fieldsToReturn.add(iter.next(), 1);
                }

                final Find.Builder fb = new Find.Builder(q);
                fb.projection(fieldsToReturn);
                fb.setLimit(1);
                fb.setBatchSize(1);
                fb.readPreference(readPreference);

                final MongoIterator<Document> ci = collection.find(fb.build());
                if (ci.hasNext()) {
                    queryResult = ci.next();
                    ci.close();
                }
            }
            else {
                queryResult = collection.findOne(q);
            }

            if (queryResult != null) {
                fillMap(result, queryResult);
            }
            return queryResult != null ? 0 : 1;
        }
        catch (final Exception e) {
            System.err.println(e.toString());
            return 1;
        }

    }

    /**
     * Perform a range scan for a set of records in the database. Each
     * field/value pair from the result will be stored in a HashMap.
     * 
     * @param table
     *            The name of the table
     * @param startkey
     *            The record key of the first record to read.
     * @param recordcount
     *            The number of records to read
     * @param fields
     *            The list of fields to read, or null for all of them
     * @param result
     *            A Vector of HashMaps, where each HashMap is a set field/value
     *            pairs for one record
     * @return Zero on success, a non-zero error code on error. See this class's
     *         description for a discussion of error codes.
     */
    @Override
    public final int scan(final String table, final String startkey,
            final int recordcount, final Set<String> fields,
            final Vector<HashMap<String, ByteIterator>> result) {
        try {
            final MongoCollection collection = db.getCollection(table);

            // { "_id":{"$gte":startKey}} }
            final Find.Builder fb = new Find.Builder();
            fb.setQuery(where("_id").greaterThanOrEqualTo(startkey));
            fb.setLimit(recordcount);
            fb.setBatchSize(recordcount);
            fb.readPreference(readPreference);
            if (fields != null) {
                final DocumentBuilder fieldsDoc = BuilderFactory.start();
                for (final String field : fields) {
                    fieldsDoc.add(field, 1);
                }

                fb.projection(fieldsDoc);
            }

            result.ensureCapacity(recordcount);
            final MongoIterator<Document> cursor = collection.find(fb.build());
            while (cursor.hasNext()) {
                // toMap() returns a Map but result.add() expects a
                // Map<String,String>. Hence, the suppress warnings.
                final Document doc = cursor.next();
                final HashMap<String, ByteIterator> docAsMap = new HashMap<String, ByteIterator>();

                fillMap(docAsMap, doc);

                result.add(docAsMap);
            }

            return 0;
        }
        catch (final Exception e) {
            System.err.println(e.toString());
            return 1;
        }
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key, overwriting any existing values with the same field name.
     * 
     * @param table
     *            The name of the table
     * @param key
     *            The record key of the record to write.
     * @param values
     *            A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error. See this class's
     *         description for a discussion of error codes.
     */
    @Override
    public final int update(final String table, final String key,
            final HashMap<String, ByteIterator> values) {
        try {
            final MongoCollection collection = db.getCollection(table);
            final DocumentBuilder q = BuilderFactory.start().add("_id", key);
            final DocumentBuilder u = BuilderFactory.start();
            final DocumentBuilder fieldsToSet = u.push("$set");
            for (final Map.Entry<String, ByteIterator> entry : values
                    .entrySet()) {
                fieldsToSet.add(entry.getKey(), entry.getValue().toArray());
            }
            final long res = collection
                    .update(q, u, false, false, writeConcern);
            return res == 1 ? 0 : 1;
        }
        catch (final Exception e) {
            System.err.println(e.toString());
            return 1;
        }
    }

    /**
     * Fills the map with the ByteIterators from the document.
     * 
     * @param result
     *            The map to fill.
     * @param queryResult
     *            The document to fill from.
     */
    protected final void fillMap(final HashMap<String, ByteIterator> result,
            final Document queryResult) {
        for (final Element be : queryResult) {
            if (be.getType() == ElementType.BINARY) {
                result.put(be.getName(), new BinaryByteArrayIterator(
                        (BinaryElement) be));
            }
        }
    }

    /**
     * BinaryByteArrayIterator provides an adapter from a {@link BinaryElement}
     * to a {@link ByteIterator}.
     */
    private final static class BinaryByteArrayIterator extends ByteIterator {

        /** The binary data. */
        private final BinaryElement binaryElement;

        /** The current offset into the binary element. */
        private int offset;

        /**
         * Creates a new BinaryByteArrayIterator.
         * 
         * @param element
         *            The {@link BinaryElement} to iterate over.
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
