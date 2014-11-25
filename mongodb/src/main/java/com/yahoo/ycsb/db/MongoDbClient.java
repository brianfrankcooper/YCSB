/**
 * MongoDB client binding for YCSB.
 *
 * Submitted by Yen Pai on 5/11/2010.
 *
 * https://gist.github.com/000a66b8db2caf42467b#file_mongo_db.java
 *
 */

package com.yahoo.ycsb.db;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.mongodb.BasicDBObject;
import com.mongodb.DBAddress;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;

import com.mongodb.*;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

/**
 * MongoDB client for YCSB framework.
 *
 * Properties to set:
 *
 * mongodb.url=mongodb://localhost:27017 mongodb.database=ycsb
 * mongodb.writeConcern=journaled
 *
 * @author ypai
 */
public class MongoDbClient extends DB {

    /** Used to include a field in a response. */
    protected static final Integer INCLUDE = 1;

    /** A singleton Mongo instance. */
    private static Mongo[] mongos;

    /** The default write concern for the test. */
    private static WriteConcern writeConcern;

    /** The default read preference for the test */
    private static ReadPreference readPreference;

    /** The database to access. */
    private static String database;

    /** Count the number of times initialized to teardown on the last {@link #cleanup()}. */
    private static final AtomicInteger initCount = new AtomicInteger(0);

    private static Random random = new Random();

    private static String[] clients = null;

    /**
     * Initialize any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override
    public void init() throws DBException {
        initCount.incrementAndGet();
        synchronized (INCLUDE) {
            if (mongos != null) {
                return;
            }

            // initialize MongoDb driver
            Properties props = getProperties();
            String urls = props.getProperty("mongodb.url",
                    "mongodb://localhost:27017");

            clients = urls.split(",");
            mongos = new Mongo[clients.length];

            database = props.getProperty("mongodb.database", "ycsb");

            // Set connectionpool to size of ycsb thread pool
            final String maxConnections = props.getProperty("threadcount", "100");

            // write concern
            String writeConcernType = props.getProperty("mongodb.writeConcern", "journaled").toLowerCase();
            if ("errors_ignored".equals(writeConcernType)) {
                writeConcern = WriteConcern.ERRORS_IGNORED;
            }
            else if ("unacknowledged".equals(writeConcernType)) {
                writeConcern = WriteConcern.UNACKNOWLEDGED;
            }
            else if ("acknowledged".equals(writeConcernType)) {
                writeConcern = WriteConcern.ACKNOWLEDGED;
            }
            else if ("journaled".equals(writeConcernType)) {
                writeConcern = WriteConcern.JOURNALED;
            }
            else if ("replica_acknowledged".equals(writeConcernType)) {
                writeConcern = WriteConcern.REPLICA_ACKNOWLEDGED;
            }
            else {
                System.err.println("ERROR: Invalid writeConcern: '"
                        + writeConcernType
                        + "'. "
                        + "Must be [ errors_ignored | unacknowledged | acknowledged | journaled | replica_acknowledged ]");
                System.exit(1);
            }

            // readPreference
            String readPreferenceType = props.getProperty("mongodb.readPreference", "primary").toLowerCase();
            if ("primary".equals(readPreferenceType)) {
                readPreference = ReadPreference.primary();
            }
            else if ("primary_preferred".equals(readPreferenceType)) {
                readPreference = ReadPreference.primaryPreferred();
            }
            else if ("secondary".equals(readPreferenceType)) {
                readPreference = ReadPreference.secondary();
            }
            else if ("secondary_preferred".equals(readPreferenceType)) {
                readPreference = ReadPreference.secondaryPreferred();
            }
            else if ("nearest".equals(readPreferenceType)) {
                readPreference = ReadPreference.nearest();
            }
            else {
                System.err.println("ERROR: Invalid readPreference: '"
                        + readPreferenceType
                        + "'. Must be [ primary | primary_preferred | secondary | secondary_preferred | nearest ]");
                System.exit(1);
            }

            for( int i=0; i< mongos.length; i++) {
                try {
                    // strip out prefix since Java driver doesn't currently support
                    // standard connection format URL yet
                    // http://www.mongodb.org/display/DOCS/Connections
                    String url = clients[i];
                    if (url.startsWith("mongodb://")) {
                        url = url.substring(10);
                    }

                    // need to append db to url.
                    url += "/" + database;
                    MongoOptions options = new MongoOptions();
                    options.setCursorFinalizerEnabled(false);
                    options.connectionsPerHost = Integer.parseInt(maxConnections);
                    mongos[i] = new Mongo(new DBAddress(url), options);

                    System.out.println("mongo connection created with " + url);
                } catch (Exception e1) {
                    System.err
                            .println("Could not initialize MongoDB connection pool for Loader: "
                                    + e1.toString());
                    e1.printStackTrace();
                    return;
                }
            }
        }
    }
    /**
     * Cleanup any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override
    public void cleanup() throws DBException {
        if (initCount.decrementAndGet() <= 0) {
            for(int i = 0; i < mongos.length; i++) {
                try {
                    mongos[i].close();
                }
                catch (Exception e1) {
                    System.err.println("Could not close MongoDB connection pool: "
                            + e1.toString());
                    e1.printStackTrace();
                    return;
                }
            }
        }
    }

    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int delete(String table, String key) {
        com.mongodb.DB db = null;
        try {
            db = mongos[random.nextInt(mongos.length)].getDB(database);
            db.requestStart();
            DBCollection collection = db.getCollection(table);
            DBObject q = new BasicDBObject().append("_id", key);
            WriteResult res = collection.remove(q, writeConcern);
            return 0;
        }
        catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        }
        finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }

    @Override
    /**
     * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key.
     *
     * @param table The name of the table
     * @param key The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    public int insert(String table, String key, Map<String, ByteIterator> values) {
        com.mongodb.DB db = null;
        try {
            db = mongos[random.nextInt(mongos.length)].getDB(database);
            db.requestStart();
            DBCollection collection = db.getCollection(table);
            DBObject r = new BasicDBObject().append("_id", key);
            for (String k : values.keySet()) {
                r.put(k, values.get(k).toArray());
            }
            WriteResult res = collection.insert(r, writeConcern);
            return 0;
        }
        catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
        finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    /**
     * Read a record from the database. Each field/value pair from the result will be stored in a Map.
     *
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param field The field to read
     * @param result A Map of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    public int readOne(String table, String key, String field, Map<String,ByteIterator> result) {

        DBObject fieldsToReturn = new BasicDBObject();
        fieldsToReturn.put(field, INCLUDE);

        return read(table, key, result, fieldsToReturn);
    }

    @Override
    @SuppressWarnings("unchecked")
    /**
     * Read a record from the database. Each field/value pair from the result will be stored in a Map.
     *
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param result A Map of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    public int readAll(String table, String key, Map<String,ByteIterator> result) {
        return read(table, key, result, null);
    }


    public int read(String table, String key, Map<String, ByteIterator> result,
            DBObject fieldsToReturn) {
        com.mongodb.DB db = null;
        try {
            db = mongos[random.nextInt(mongos.length)].getDB(database);

            db.requestStart();

            DBCollection collection = db.getCollection(table);
            DBObject q = new BasicDBObject().append("_id", key);
            DBObject queryResult = collection.findOne(q, fieldsToReturn, readPreference);

            if (queryResult != null) {
                result.putAll(resultify(queryResult));
            }
            return queryResult != null ? 0 : 1;
        } catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        } finally {
            if (db!=null)
            {
                db.requestDone();
            }
        }
    }

    @Override
    /**
     * Update a record in the database. Any field/value pairs in the specified values Map will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     * @param table The name of the table
     * @param key The record key of the record to write.
     * @param value The value to update in the key record
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
     */
    public int updateOne(String table, String key, String field, ByteIterator value) {

        DBObject fieldsToSet = new BasicDBObject();
        fieldsToSet.put(key, value.toArray());

        return update(table, key, fieldsToSet);
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified values Map will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     * @param table The name of the table
     * @param key The record key of the record to write.
     * @param values A Map of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
     */
    public int updateAll(String table, String key, Map<String,ByteIterator> values) {

        DBObject fieldsToSet = new BasicDBObject();
        Iterator<String> keys = values.keySet().iterator();
        while (keys.hasNext()) {
            String tmpKey = keys.next();
            fieldsToSet.put(tmpKey, values.get(tmpKey).toArray());
        }

        return update(table, key, fieldsToSet);
    }

    public int update(String table, String key, DBObject fieldsToSet) {
        com.mongodb.DB db = null;
        try {
            db = mongos[random.nextInt(mongos.length)].getDB(database);

            db.requestStart();

            DBCollection collection = db.getCollection(table);
            DBObject q = new BasicDBObject().append("_id", key);
            DBObject u = new BasicDBObject();

            u.put("$set", fieldsToSet);
            WriteResult res = collection.update(q, u, false, false,
                    writeConcern);
            return 0;
        } catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        } finally {
            if (db!=null)
            {
                db.requestDone();
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    /**
     * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a Map.
     *
     * @param table The name of the table
     * @param startkey The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param result A List of Maps, where each Map is a set field/value pairs for one record
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
     */
    public int scanAll(String table, String startkey, int recordcount, List<Map<String, ByteIterator>> result) {

        return scan(table, startkey, recordcount, result);
    }

    @Override
    @SuppressWarnings("unchecked")
    /**
     * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a Map.
     *
     * @param table The name of the table
     * @param startkey The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param field The field to read
     * @param result A List of Maps, where each Map is a set field/value pairs for one record
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
     */
    public int scanOne(String table, String startkey, int recordcount, String field, List<Map<String, ByteIterator>> result) {

        return scan(table, startkey, recordcount, result);
    }

    public int scan(String table, String startkey, int recordcount,
            List<Map<String, ByteIterator>> result) {
        com.mongodb.DB db = null;
        DBCursor cursor = null;
        try {
            db = mongos[random.nextInt(mongos.length)].getDB(database);
            db.requestStart();
            DBCollection collection = db.getCollection(table);
            // { "_id":{"$gte":startKey, "$lte":{"appId":key+"\uFFFF"}} }
            DBObject scanRange = new BasicDBObject().append("$gte", startkey);
            DBObject q = new BasicDBObject().append("_id", scanRange);
            cursor = collection.find(q).limit(recordcount);    //TODO: apply readPreference here
            while (cursor.hasNext()) {
                result.add(resultify(cursor.next()));
            }

            return 0;
        } catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        }
        finally
        {
            if (db!=null)
            {
                db.requestDone();
            }
        }

    }

    /**
     * Turn everything in the object into a ByteIterator
     */
    @SuppressWarnings("unchecked")
    private HashMap<String, ByteIterator> resultify(DBObject object) {
        HashMap<String, ByteIterator> decoded = new HashMap<String, ByteIterator>();

        for (Map.Entry<String, Object> entry : ((Map<String, Object>) object.toMap()).entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (key.equals("_id")) {
                try {
                    decoded.put(key, new ByteArrayByteIterator(((String) value).getBytes("UTF-8")));
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                }
            }
            else {
                decoded.put(key, new ByteArrayByteIterator((byte[]) value));
            }
        }

        return decoded;
    }
}

