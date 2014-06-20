/**
 * MongoDB client binding for YCSB.
 *
 * Submitted by Yen Pai on 5/11/2010.
 *
 * https://gist.github.com/000a66b8db2caf42467b#file_mongo_db.java
 *
 */

package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

/**
 * MongoDB client for YCSB framework.
 * 
 * Properties to set:
 * 
 * mongodb.url=mongodb://localhost:27017 mongodb.database=ycsb
 * mongodb.writeConcern=acknowledged
 * 
 * @author ypai
 */
public class MongoDbClient extends DB {

    /** Used to include a field in a response. */
    protected static final Integer INCLUDE = Integer.valueOf(1);

    /** The database to access. */
    private static String database;

    /**
     * Count the number of times initialized to teardown on the last
     * {@link #cleanup()}.
     */
    private static final AtomicInteger initCount = new AtomicInteger(0);

    /** A singleton Mongo instance. */
    private static MongoClient mongo;

    /** The default read preference for the test */
    private static ReadPreference readPreference;

    /** The default write concern for the test. */
    private static WriteConcern writeConcern;

    /**
     * Cleanup any state for this DB. Called once per DB instance; there is one
     * DB instance per client thread.
     */
    @Override
    public void cleanup() throws DBException {
        if (initCount.decrementAndGet() <= 0) {
            try {
                mongo.close();
            }
            catch (Exception e1) {
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
    public int delete(String table, String key) {
        MongoDatabase db = null;
        try {
            db = mongo.getDatabase(database);
            MongoCollection<Document> collection = db.getCollection(table);

            Document q = new Document("_id", key);
            collection.withWriteConcern(writeConcern).deleteOne(q);

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
            if (mongo != null) {
                return;
            }

            // Just use the standard connection format URL
            // http://docs.mongodb.org/manual/reference/connection-string/
            //
            // Support legacy options by updating the URL as appropriate.
            Properties props = getProperties();
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
                                + "See http://docs.mongodb.org/manual/reference/connection-string/.");
                System.exit(1);
            }

            try {
                MongoClientURI uri = new MongoClientURI(url);

                String uriDb = uri.getDatabase();
                if (!defaultedUrl && (uriDb != null) && !uriDb.isEmpty()
                        && !"admin".equals(uriDb)) {
                    database = uriDb;
                }
                else {
                    database = props.getProperty("mongodb.database", "ycsb");
                }

                if ((database == null) || database.isEmpty()) {
                    System.err
                            .println("ERROR: Invalid URL: '"
                                    + url
                                    + "'. Must provide a database name with the URI. "
                                    + "'mongodb://<host1>:<port1>,<host2>:<port2>/database");
                    System.exit(1);
                }

                readPreference = uri.getOptions().getReadPreference();
                writeConcern = uri.getOptions().getWriteConcern();

                mongo = new MongoClient(uri);

                System.out.println("mongo connection created with " + url);
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
     * @return Zero on success, a non-zero error code on error. See this class's
     *         description for a discussion of error codes.
     */
    @Override
    public int insert(String table, String key,
            HashMap<String, ByteIterator> values) {
        MongoDatabase db = null;
        try {
            db = mongo.getDatabase(database);

            MongoCollection<Document> collection = db.getCollection(table);
            Document criteria = new Document("_id", key);
            Document toInsert = new Document("_id", key);
            for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
                toInsert.put(entry.getKey(), entry.getValue().toArray());
            }

            collection.withWriteConcern(writeConcern).updateOne(criteria,
                    toInsert, new UpdateOptions().upsert(true));

            return 0;
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
        MongoDatabase db = null;
        try {
            db = mongo.getDatabase(database);

            MongoCollection<Document> collection = db.getCollection(table);
            Document q = new Document("_id", key);
            Document fieldsToReturn = new Document();

            Document queryResult = null;
            if (fields != null) {
                Iterator<String> iter = fields.iterator();
                while (iter.hasNext()) {
                    fieldsToReturn.put(iter.next(), INCLUDE);
                }
                queryResult = collection.withReadPreference(readPreference)
                        .find(q).projection(fieldsToReturn).first();
            }
            else {
                queryResult = collection.withReadPreference(readPreference)
                        .find(q).first();
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
     * @return Zero on success, a non-zero error code on error. See this class's
     *         description for a discussion of error codes.
     */
    @Override
    public int scan(String table, String startkey, int recordcount,
            Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        MongoDatabase db = null;
        FindIterable<Document> cursor = null;
        MongoCursor<Document> iter = null;
        try {
            db = mongo.getDatabase(database);

            MongoCollection<Document> collection = db.getCollection(table);

            // { "_id":{"$gte":startKey, "$lte":{"appId":key+"\uFFFF"}} }
            Document scanRange = new Document("$gte", startkey);
            Document q = new Document("_id", scanRange);
            cursor = collection.withReadPreference(readPreference).find(q)
                    .limit(recordcount);

            iter = cursor.iterator();
            while (iter.hasNext()) {
                // toMap() returns a Map, but result.add() expects a
                // Map<String,String>. Hence, the suppress warnings.
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
        MongoDatabase db = null;
        try {
            db = mongo.getDatabase(database);

            MongoCollection<Document> collection = db.getCollection(table);
            Document q = new Document("_id", key);

            Document fieldsToSet = new Document();
            Iterator<String> keys = values.keySet().iterator();
            while (keys.hasNext()) {
                String tmpKey = keys.next();
                fieldsToSet.put(tmpKey, values.get(tmpKey).toArray());
            }
            Document u = new Document("$set", fieldsToSet);

            collection.withWriteConcern(writeConcern).updateOne(q, u);
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
            if (entry.getValue() instanceof byte[]) {
                resultMap.put(entry.getKey(), new ByteArrayByteIterator(
                        (byte[]) entry.getValue()));
            }
        }
    }
}
