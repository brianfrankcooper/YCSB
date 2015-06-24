/**
 * MongoDB client binding for YCSB.
 *
 * Submitted by Yen Pai on 5/11/2010.
 *
 * https://gist.github.com/000a66b8db2caf42467b#file_mongo_database.java
 *
 */

package com.yahoo.ycsb.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import org.bson.Document;
import org.bson.types.Binary;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

/**
 * MongoDB client for YCSB framework.
 * 
 * Properties to set:
 * 
 * mongodb.url=mongodb://localhost:27017
 * mongodb.writeConcern=acknowledged
 * 
 * @author ypai
 */
public class MongoDbClient extends DB {

    /** Update options to do an upsert. */
    private static final UpdateOptions UPSERT = new UpdateOptions()
            .upsert(true);

    /** Used to include a field in a response. */
    protected static final Integer INCLUDE = Integer.valueOf(1);

    /** The database name to access. */
    private static String databaseName;

    /** The database name to access. */
    private static MongoDatabase database;

    /**
     * Count the number of times initialized to teardown on the last
     * {@link #cleanup()}.
     */
    private static final AtomicInteger initCount = new AtomicInteger(0);

    /** A singleton Mongo instance. */
    private static MongoClient mongoClient;

    /** The default read preference for the test */
    private static ReadPreference readPreference;

    /** The default write concern for the test. */
    private static WriteConcern writeConcern;

    /** The batch size to use for inserts. */
    private static int batchSize;

    /** The bulk inserts pending for the thread. */
    private final List<InsertOneModel<Document>> bulkInserts = new ArrayList<InsertOneModel<Document>>();

    /**
     * Cleanup any state for this DB. Called once per DB instance; there is one
     * DB instance per client thread.
     */
    @Override
    public void cleanup() throws DBException {
        if (initCount.decrementAndGet() == 0) {
            try {
                mongoClient.close();
            }
            catch (Exception e1) {
                System.err.println("Could not close MongoDB connection pool: "
                        + e1.toString());
                e1.printStackTrace();
                return;
            }
            finally {
                database = null;
                mongoClient = null;
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
     * @return Zero on success, a non-zero error code on error. See the
     *         {@link DB} class's description for a discussion of error codes.
     */
    @Override
    public int delete(String table, String key) {
        try {
            MongoCollection<Document> collection = database
                    .getCollection(table);

            Document query = new Document("_id", key);
            DeleteResult result = collection.withWriteConcern(writeConcern)
                    .deleteOne(query);
            if (result.wasAcknowledged() && result.getDeletedCount() == 0) {
                System.err.println("Nothing deleted for key " + key);
                return 1;
            }
            return 0;
        }
        catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        }
    }

    /**
     * Initialize any state for this DB. Called once per DB instance; there is
     * one DB instance per client thread.
     */
    @Override
    public void init() throws DBException {
        initCount.incrementAndGet();
        synchronized (INCLUDE) {
            if (mongoClient != null) {
                return;
            }

            Properties props = getProperties();

            // Set insert batchsize, default 1 - to be YCSB-original equivalent
            batchSize = Integer.parseInt(props.getProperty("batchsize", "1"));

            // Just use the standard connection format URL
            // http://docs.mongodb.org/manual/reference/connection-string/ 
            // to configure the client.
            String url = props.getProperty("mongodb.url", null);
            boolean defaultedUrl = false;
            if (url == null) {
                defaultedUrl = true;
                url = "mongodb://localhost:27017/ycsb?w=1";
            }

            url = OptionsSupport.updateUrl(url, props);

            if (!url.startsWith("mongodb://")) {
                System.err
                        .println("ERROR: Invalid URL: '"
                                + url
                                + "'. Must be of the form "
                                + "'mongodb://<host1>:<port1>,<host2>:<port2>/database?options'. "
                                + "http://docs.mongodb.org/manual/reference/connection-string/");
                System.exit(1);
            }

            try {
                MongoClientURI uri = new MongoClientURI(url);

                String uriDb = uri.getDatabase();
                if (!defaultedUrl && (uriDb != null) && !uriDb.isEmpty()
                        && !"admin".equals(uriDb)) {
                    databaseName = uriDb;
                }
                else {
                    //If no database is specified in URI, use "ycsb"
                    databaseName = "ycsb"; 

                }


                readPreference = uri.getOptions().getReadPreference();
                writeConcern = uri.getOptions().getWriteConcern();

                mongoClient = new MongoClient(uri);
                database = mongoClient.getDatabase(databaseName);

                System.out.println("mongo client connection created with "
                        + url);
            }
            catch (Exception e1) {
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
     * @return Zero on success, a non-zero error code on error. See the
     *         {@link DB} class's description for a discussion of error codes.
     */
    @Override
    public int insert(String table, String key,
            HashMap<String, ByteIterator> values) {
        try {
            MongoCollection<Document> collection = database
                    .getCollection(table);
            Document criteria = new Document("_id", key);
            Document toInsert = new Document("_id", key);
            for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
                toInsert.put(entry.getKey(), entry.getValue().toArray());
            }

            // Do a single upsert.
            if (batchSize <= 1) {
                UpdateResult result = collection.withWriteConcern(writeConcern)
                        .replaceOne(criteria, toInsert, UPSERT);
                if (!result.wasAcknowledged()
                        || result.getMatchedCount() > 0
                        || (result.isModifiedCountAvailable() && (result
                                .getModifiedCount() > 0))
                        || result.getUpsertedId() != null) {
                    return 0;
                }

                System.err.println("Nothing inserted for key " + key);
                return 1;
            }

            // Use a bulk insert.
            try {
                bulkInserts.add(new InsertOneModel<Document>(toInsert));
                if (bulkInserts.size() < batchSize) {
                    return 0;
                }

                BulkWriteResult result = collection.withWriteConcern(
                        writeConcern).bulkWrite(bulkInserts,
                        new BulkWriteOptions().ordered(false));
                if (!result.wasAcknowledged()
                        || result.getInsertedCount() == bulkInserts.size()) {
                    bulkInserts.clear();
                    return 0;
                }

                System.err
                        .println("Number of inserted documents doesn't match the number sent, "
                                + result.getInsertedCount()
                                + " inserted, sent " + bulkInserts.size());
                bulkInserts.clear();
                return 1;
            }
            catch (Exception e) {
                System.err.println("Exception while trying bulk insert with "
                        + bulkInserts.size());
                e.printStackTrace();
                return 1;
            }
        }
        catch (Exception e) {
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
    public int read(String table, String key, Set<String> fields,
            HashMap<String, ByteIterator> result) {
        try {
            MongoCollection<Document> collection = database
                    .getCollection(table);
            Document query = new Document("_id", key);
            Document fieldsToReturn = new Document();

            Document queryResult = null;
            if (fields != null) {
                Iterator<String> iter = fields.iterator();
                while (iter.hasNext()) {
                    fieldsToReturn.put(iter.next(), INCLUDE);
                }
                queryResult = collection.withReadPreference(readPreference)
                        .find(query).projection(fieldsToReturn).first();
            }
            else {
                queryResult = collection.withReadPreference(readPreference)
                        .find(query).first();
            }

            if (queryResult != null) {
                fillMap(result, queryResult);
            }
            return queryResult != null ? 0 : 1;
        }
        catch (Exception e) {
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
     * @return Zero on success, a non-zero error code on error. See the
     *         {@link DB} class's description for a discussion of error codes.
     */
    @Override
    public int scan(String table, String startkey, int recordcount,
            Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        FindIterable<Document> cursor = null;
        MongoCursor<Document> iter = null;
        try {
            MongoCollection<Document> collection = database
                    .getCollection(table);

            Document scanRange = new Document("$gte", startkey);
            Document query = new Document("_id", scanRange);
            Document sort = new Document("_id", INCLUDE);
            Document projection = null;
            if (fields != null) {
                projection = new Document();
                for (String fieldName : fields) {
                    projection.put(fieldName, INCLUDE);
                }
            }

            cursor = collection.withReadPreference(readPreference).find(query)
                    .projection(projection).sort(sort).limit(recordcount);

            // Do the query.
            iter = cursor.iterator();
            if (!iter.hasNext()) {
                System.err.println("Nothing found in scan for key " + startkey);
                return 1;
            }
            while (iter.hasNext()) {
                HashMap<String, ByteIterator> resultMap = new HashMap<String, ByteIterator>();

                Document obj = iter.next();
                fillMap(resultMap, obj);

                result.add(resultMap);
            }

            return 0;
        }
        catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        }
        finally {
            if (iter != null) {
                iter.close();
            }
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
    public int update(String table, String key,
            HashMap<String, ByteIterator> values) {
        try {
            MongoCollection<Document> collection = database
                    .getCollection(table);

            Document query = new Document("_id", key);
            Document fieldsToSet = new Document();
            for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
                fieldsToSet.put(entry.getKey(), entry.getValue().toArray());
            }
            Document update = new Document("$set", fieldsToSet);

            UpdateResult result = collection.withWriteConcern(writeConcern)
                    .updateOne(query, update);
            if (result.wasAcknowledged() && result.getMatchedCount() == 0) {
                System.err.println("Nothing updated for key " + key);
                return 1;
            }
            return 0;
        }
        catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        }
    }

    /**
     * Fills the map with the values from the DBObject.
     * 
     * @param resultMap
     *            The map to fill/
     * @param obj
     *            The object to copy values from.
     */
    protected void fillMap(HashMap<String, ByteIterator> resultMap, Document obj) {
        for (Map.Entry<String, Object> entry : obj.entrySet()) {
            if (entry.getValue() instanceof Binary) {
                resultMap.put(entry.getKey(), new ByteArrayByteIterator(
                        ((Binary) entry.getValue()).getData()));
            }
        }
    }
}
