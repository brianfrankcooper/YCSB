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

import com.mongodb.*;
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
 * mongodb.database=ycsb
 * mongodb.writeConcern=(none|normal|safe|fsync_safe|replicas_safe)
 * mongodb.writeConcern.w=(-1|0|1|2)
 * mongodb.writeConcern.wtimeout=(0|other value in ms)
 * mongodb.writeConcern.fsync=(true|false)
 * mongodb.writeConcern.j=(true|false)
 * mongodb.writeConcern.continueOnInsertError=(true|false)
 * mongodb.readPreference=secondaryPreferred
 *
 * mongodb.writeConcert.X override specific parameters defined by general writeConcern property.
 *
 * Default values:
 *
 * mongodb.url=mongodb://localhost:27017
 * mongodb.database=ycsb
 * mongodb.writeConcern=safe
 * mongodb.readPreference=primary
 *
 * @link http://api.mongodb.org/java/2.10.1/com/mongodb/WriteConcern.html
 * @link http://api.mongodb.org/java/2.10.1/com/mongodb/ReadPreference.html
 * @author ypai
 * @author dnelubin
 */
public class MongoDbClient extends DB {

    /** Used to include a field in a response. */
    protected static final Integer INCLUDE = Integer.valueOf(1);

    /** A singleton Mongo instance. */
    private static Mongo mongo;

    /** The default write concern for the test. */
    private static WriteConcern writeConcern;

    /** The default read preference for the test. */
    private static ReadPreference readPreference;

    /** The database to access. */
    private static String database;

    /** Count the number of times initialized to teardown on the last {@link #cleanup()}. */
    private static final AtomicInteger initCount = new AtomicInteger(0);

    /**
     * Initialize any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override
    public void init() throws DBException {
        initCount.incrementAndGet();
        synchronized (INCLUDE) {
            if (mongo != null) {
                return;
            }

            // initialize MongoDb driver
            Properties props = getProperties();
            String url = props.getProperty("mongodb.url",
                    "mongodb://localhost:27017");
            database = props.getProperty("mongodb.database", "ycsb");
            String writeConcernType = props.getProperty("mongodb.writeConcern",
                    "safe").toLowerCase();
            String readPreferenceType = props.getProperty("mongodb.readPreference",
                    "primary").toLowerCase();
            final String maxConnections = props.getProperty(
                    "mongodb.maxconnections", "10");

            if ("none".equals(writeConcernType)) {
                writeConcern = WriteConcern.NONE;
            }
            else if ("safe".equals(writeConcernType)) {
                writeConcern = WriteConcern.SAFE;
            }
            else if ("normal".equals(writeConcernType)) {
                writeConcern = WriteConcern.NORMAL;
            }
            else if ("fsync_safe".equals(writeConcernType)) {
                writeConcern = WriteConcern.FSYNC_SAFE;
            }
            else if ("replicas_safe".equals(writeConcernType)) {
                writeConcern = WriteConcern.REPLICAS_SAFE;
            }
            else {
                System.err
                        .println("ERROR: Invalid writeConcern: '"
                                + writeConcernType
                                + "'. "
                                + "Must be [ none | safe | normal | fsync_safe | replicas_safe ]");
                System.exit(1);
            }

            String writeConcernWValue = props.getProperty("mongodb.writeConcern.w", String.valueOf(writeConcern.getW()));
            String writeConcernWtimeoutValue = props.getProperty("mongodb.writeConcern.wtimeout",
                    String.valueOf(writeConcern.getWtimeout()));
            String writeConcernFsyncValue = props.getProperty("mongodb.writeConcern.fsync", String.valueOf(writeConcern.getFsync()));
            String writeConcernJValue = props.getProperty("mongodb.writeConcern.j", String.valueOf(writeConcern.getJ()));
            String writeConcernContinueValue = props.getProperty("mongodb.writeConcern.continueOnErrorForInsert",
                    String.valueOf(writeConcern.getContinueOnErrorForInsert()));

            try {
                writeConcern = new WriteConcern(Integer.parseInt(writeConcernWValue),
                        writeConcern.getWtimeout(),
                        writeConcern.getFsync(),
                        writeConcern.getJ(),
                        writeConcern.getContinueOnErrorForInsert());
            } catch (NumberFormatException e) {
                System.err.println("ERROR: Invalid writeConcern.w: '" + writeConcernWValue + "'. " +
                        "Must be integer");
                System.exit(1);
            }

            try {
                writeConcern = new WriteConcern(writeConcern.getW(),
                        Integer.parseInt(writeConcernWtimeoutValue),
                        writeConcern.getFsync(),
                        writeConcern.getJ(),
                        writeConcern.getContinueOnErrorForInsert());
            } catch (NumberFormatException e) {
                System.err.println("ERROR: Invalid writeConcern.wtimeout: '" + writeConcernWtimeoutValue + "'. " +
                        "Must be integer");
                System.exit(1);
            }

            if (!"true".equalsIgnoreCase(writeConcernFsyncValue) && !"false".equalsIgnoreCase(writeConcernFsyncValue)) {
                System.err.println("ERROR: Invalid writeConcern.fsync: '" + writeConcernFsyncValue + "'. " +
                        "Must be true or false");
                System.exit(1);
            }
            writeConcern = new WriteConcern(writeConcern.getW(),
                    writeConcern.getWtimeout(),
                    Boolean.parseBoolean(writeConcernFsyncValue),
                    writeConcern.getJ(),
                    writeConcern.getContinueOnErrorForInsert());

            if (!"true".equalsIgnoreCase(writeConcernJValue) && !"false".equalsIgnoreCase(writeConcernJValue)) {
                System.err.println("ERROR: Invalid writeConcern.j: '" + writeConcernJValue + "'. " +
                        "Must be true or false");
                System.exit(1);
            }
            writeConcern = new WriteConcern(writeConcern.getW(),
                    writeConcern.getWtimeout(),
                    writeConcern.getFsync(),
                    Boolean.parseBoolean(writeConcernJValue),
                    writeConcern.getContinueOnErrorForInsert());

            if (!"true".equalsIgnoreCase(writeConcernContinueValue) && !"false".equalsIgnoreCase(writeConcernContinueValue)) {
                System.err.println("ERROR: Invalid writeConcern.continueOnErrorForInsert: '" + writeConcernContinueValue + "'. " +
                        "Must be true or false");
                System.exit(1);
            }
            writeConcern = new WriteConcern(writeConcern.getW(),
                    writeConcern.getWtimeout(),
                    writeConcern.getFsync(),
                    writeConcern.getJ(),
                    Boolean.parseBoolean(writeConcernContinueValue));

            if ("primary".equals(readPreferenceType)) {
                readPreference = ReadPreference.primary();
            }
            else if ("primarypreferred".equals(readPreferenceType)) {
                readPreference = ReadPreference.primaryPreferred();
            }
            else if ("secondary".equals(readPreferenceType)) {
                readPreference = ReadPreference.secondary();
            }
            else if ("secondarypreferred".equals(readPreferenceType)) {
                readPreference = ReadPreference.secondaryPreferred();
            }
            else if ("nearest".equals(readPreferenceType)) {
                readPreference = ReadPreference.nearest();
            }
            else {
                System.err
                        .println("ERROR: Invalid readPreference: '"
                                + readPreferenceType
                                + "'. "
                                + "Must be [ primary | primaryPreferred | secondary | secondaryPreferred | nearest ]");
                System.exit(1);
            }
            //TODO: support tagset

            try {
                // strip out prefix since Java driver doesn't currently support
                // standard connection format URL yet
                // http://www.mongodb.org/display/DOCS/Connections
                if (url.startsWith("mongodb://")) {
                    url = url.substring(10);
                }

                // need to append db to url.
                url += "/" + database;
                System.out.println("new database url = " + url);
                MongoOptions options = new MongoOptions();
                options.connectionsPerHost = Integer.parseInt(maxConnections);
                mongo = new Mongo(new DBAddress(url), options);

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
     * Cleanup any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
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
     * @param table The name of the table
     * @param key The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int delete(String table, String key) {
        com.mongodb.DB db = null;
        try {
            db = mongo.getDB(database);
            db.requestStart();
            DBCollection collection = db.getCollection(table);
            DBObject q = new BasicDBObject().append("_id", key);
            WriteResult res = collection.remove(q, writeConcern);
            return res.getN() == 1 ? 0 : 1;
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

    /**
     * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key.
     *
     * @param table The name of the table
     * @param key The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int insert(String table, String key,
            HashMap<String, ByteIterator> values) {
        com.mongodb.DB db = null;
        try {
            db = mongo.getDB(database);

            db.requestStart();

            DBCollection collection = db.getCollection(table);
            DBObject r = new BasicDBObject().append("_id", key);
            for (String k : values.keySet()) {
                r.put(k, values.get(k).toArray());
            }
            WriteResult res = collection.insert(r, writeConcern);
            String error = res.getError();
            if (error == null) {
                return 0;
            } else {
                System.err.println(error);
                return 1;
            }
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

    /**
     * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    @Override
    @SuppressWarnings("unchecked")
    public int read(String table, String key, Set<String> fields,
            HashMap<String, ByteIterator> result) {
        com.mongodb.DB db = null;
        try {
            db = mongo.getDB(database);

            db.requestStart();

            DBCollection collection = db.getCollection(table);
            DBObject q = new BasicDBObject().append("_id", key);
            DBObject fieldsToReturn = new BasicDBObject();

            DBObject queryResult = null;
            if (fields != null) {
                Iterator<String> iter = fields.iterator();
                while (iter.hasNext()) {
                    fieldsToReturn.put(iter.next(), INCLUDE);
                }
                queryResult = collection.findOne(q, fieldsToReturn, readPreference);
            }
            else {
                queryResult = collection.findOne(q, null, readPreference);
            }

            if (queryResult != null) {
                result.putAll(queryResult.toMap());
            }
            return queryResult != null ? 0 : 1;
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

    /**
     * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     * @param table The name of the table
     * @param key The record key of the record to write.
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int update(String table, String key,
            HashMap<String, ByteIterator> values) {
        com.mongodb.DB db = null;
        try {
            db = mongo.getDB(database);

            db.requestStart();

            DBCollection collection = db.getCollection(table);
            DBObject q = new BasicDBObject().append("_id", key);
            DBObject u = new BasicDBObject();
            DBObject fieldsToSet = new BasicDBObject();
            Iterator<String> keys = values.keySet().iterator();
            while (keys.hasNext()) {
                String tmpKey = keys.next();
                fieldsToSet.put(tmpKey, values.get(tmpKey).toArray());

            }
            u.put("$set", fieldsToSet);
            WriteResult res = collection.update(q, u, false, false,
                    writeConcern);
            String error = res.getError();
            if (error != null) {
                System.err.println(error);
            }
            return res.getN() == 1 ? 0 : 1;
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

    /**
     * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param startkey The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param fields The list of fields to read, or null for all of them
     * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int scan(String table, String startkey, int recordcount,
            Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        com.mongodb.DB db = null;
        try {
            db = mongo.getDB(database);
            db.requestStart();
            DBCollection collection = db.getCollection(table);
            // { "_id":{"$gte":startKey, "$lte":{"appId":key+"\uFFFF"}} }
            DBObject scanRange = new BasicDBObject().append("$gte", startkey);
            DBObject q = new BasicDBObject().append("_id", scanRange);
            DBCursor cursor = collection.find(q).limit(recordcount);    //TODO: apply readPreference here
            while (cursor.hasNext()) {
                // toMap() returns a Map, but result.add() expects a
                // Map<String,String>. Hence, the suppress warnings.
                HashMap<String, ByteIterator> resultMap = new HashMap<String, ByteIterator>();

                DBObject obj = cursor.next();
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
            if (db != null) {
                db.requestDone();
            }
        }

    }

    /**
     * TODO - Finish
     * 
     * @param resultMap
     * @param obj
     */
    @SuppressWarnings("unchecked")
    protected void fillMap(HashMap<String, ByteIterator> resultMap, DBObject obj) {
        Map<String, Object> objMap = obj.toMap();
        for (Map.Entry<String, Object> entry : objMap.entrySet()) {
            if (entry.getValue() instanceof byte[]) {
                resultMap.put(entry.getKey(), new ByteArrayByteIterator(
                        (byte[]) entry.getValue()));
            }
        }
    }
}
