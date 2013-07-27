/**
 * MongoDB client binding for YCSB using the Asynchronous Java Driver.
 *
 * Submitted by Rob Moore.
 */

package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import com.allanbank.mongodb.MongoIterator;
import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.LockType;
import com.allanbank.mongodb.Mongo;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoDatabase;
import com.allanbank.mongodb.MongoDbUri;
import com.allanbank.mongodb.MongoFactory;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.BinaryElement;
import com.allanbank.mongodb.builder.Find;
import com.allanbank.mongodb.builder.QueryBuilder;
import com.yahoo.ycsb.ByteArrayByteIterator;
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
 * 
 */
public class AsyncMongoDbClient extends DB {

    /** The database to use. */
    private static String database;

    /** The connection to MongoDB. */
    private static Mongo mongo;

    /** The database to MongoDB. */
    private MongoDatabase db;

    /** The write concern for the requests. */
    private static Durability writeConcern;

    /** The write concern for the requests. */
    private static final AtomicInteger initCount = new AtomicInteger(0);

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
        initCount.incrementAndGet();
        synchronized (AsyncMongoDbClient.class) {
            if (mongo != null) {
                db = mongo.getDatabase(database);
                return;
            }

            // initialize MongoDb driver
            final Properties props = getProperties();
            String url = props.getProperty("mongodb.url",
                    "mongodb://localhost:27017");
            database = props.getProperty("mongodb.database", "ycsb");
            final String writeConcernType = props.getProperty(
                    "mongodb.writeConcern",
                    props.getProperty("mongodb.durability", "safe"))
                    .toLowerCase();
            final String maxConnections = props.getProperty(
                    "mongodb.maxconnections", "10");

            if ("none".equals(writeConcernType)) {
                writeConcern = Durability.NONE;
            }
            else if ("safe".equals(writeConcernType)) {
                writeConcern = Durability.ACK;
            }
            else if ("normal".equals(writeConcernType)) {
                writeConcern = Durability.ACK;
            }
            else if ("fsync_safe".equals(writeConcernType)) {
                writeConcern = Durability.fsyncDurable(10000);
            }
            else if ("replicas_safe".equals(writeConcernType)) {
                writeConcern = Durability.replicaDurable(10000);
            }
            else {
                System.err
                        .println("ERROR: Invalid durability: '"
                                + writeConcernType
                                + "'. "
                                + "Must be [ none | safe | normal | fsync_safe | replicas_safe ]");
                System.exit(1);
            }

            try {
                // need to append db to url.
                url += "/" + database;
                System.out.println("new database url = " + url);
                mongo = MongoFactory.create(new MongoDbUri(url));
                mongo.getConfig().setMaxConnectionCount(
                        Integer.parseInt(maxConnections));
                mongo.getConfig().setLockType(LockType.LOW_LATENCY_SPIN);
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
            final DocumentBuilder r = BuilderFactory.start().add("_id", key);
            for (final Map.Entry<String, ByteIterator> entry : values
                    .entrySet()) {
                r.add(entry.getKey(), entry.getValue().toArray());
            }
            collection.insert(writeConcern, r);
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
            final Document q = BuilderFactory.start().add("_id", key).build();
            final DocumentBuilder fieldsToReturn = BuilderFactory.start();

            Document queryResult = null;
            if (fields != null) {
                final Iterator<String> iter = fields.iterator();
                while (iter.hasNext()) {
                    fieldsToReturn.add(iter.next(), 1);
                }

                final Find.Builder fb = new Find.Builder(q);
                fb.setReturnFields(fieldsToReturn);
                fb.setLimit(1);
                fb.setBatchSize(1);

                final MongoIterator<Document> ci = collection.find(fb
                        .build());
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

            // { "_id":{"$gte":startKey, "$lte":{"appId":key+"\uFFFF"}} }
            final Find.Builder fb = new Find.Builder();
            fb.setQuery(QueryBuilder.where("_id")
                    .greaterThanOrEqualTo(startkey));
            fb.setLimit(recordcount);
            fb.setBatchSize(recordcount);
            if (fields != null) {
                DocumentBuilder fieldsDoc = BuilderFactory.start();
                for (String field : fields) {
                    fieldsDoc.add(field, 1);
                }

                fb.setReturnFields(fieldsDoc);
            }

            result.ensureCapacity(recordcount);
            final MongoIterator<Document> cursor = collection.find(fb
                    .build());
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
            final Document q = BuilderFactory.start().add("_id", key).build();
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
            if (be instanceof BinaryElement) {
                result.put(be.getName(), new ByteArrayByteIterator(
                        ((BinaryElement) be).getValue()));
            }
        }
    }
}
