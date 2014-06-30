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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.Map;
import java.util.Vector;

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
 * mongodb.url=mongodb://localhost:27017
 * mongodb.database=ycsb
 * mongodb.writeConcern=(none|normal|safe|fsync_safe|replicas_safe|custom)
 *
 * If custom:
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
 *
 */
public class MongoDbClient extends DB {

    Mongo mongo;
    WriteConcern writeConcern;
    ReadPreference readPreference;
    String database;

    @Override
    /**
     * Initialize any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    public void init() throws DBException {
        // initialize MongoDb driver
        Properties props = getProperties();
        String url = props.getProperty("mongodb.url", "mongodb://localhost:27017");
        database = props.getProperty("mongodb.database", "ycsb");
        String writeConcernValue = props.getProperty("mongodb.writeConcern", "FSYNC_SAFE").toUpperCase();
        String readPreferenceValue = props.getProperty("mongodb.readPreference", "NEAREST").toUpperCase();

        if (writeConcernValue.equals("CUSTOM")) {
            String writeConcernWValue = props.getProperty("mongodb.writeConcern.w", String.valueOf(writeConcern.getW()));
            String writeConcernWtimeoutValue = props.getProperty("mongodb.writeConcern.wtimeout",
                                                                 String.valueOf(writeConcern.getWtimeout()));
            String writeConcernFsyncValue = props.getProperty("mongodb.writeConcern.fsync", String.valueOf(writeConcern.getFsync()));
            String writeConcernJValue = props.getProperty("mongodb.writeConcern.j", String.valueOf(writeConcern.getJ()));
            String writeConcernContinueValue = props.getProperty("mongodb.writeConcern.continueOnErrorForInsert",
                                                                 String.valueOf(writeConcern.getContinueOnErrorForInsert()));

            int writeConcernWInt = Integer.MIN_VALUE;
            try {
                writeConcernWInt = Integer.parseInt(writeConcernWValue);
            } catch (NumberFormatException e) {
                System.err.println("ERROR: Invalid writeConcern.w: '" + writeConcernWValue + "'. Must be integer");
                System.exit(1);
            }

            int writeTimeoutInt = Integer.MIN_VALUE;
            try {
                writeTimeoutInt = Integer.parseInt(writeConcernWtimeoutValue);
            } catch (NumberFormatException e) {
                System.err.println("ERROR: Invalid writeConcern.wtimeout: '" + writeConcernWtimeoutValue + "'. " +
                                   "Must be integer");
                System.exit(1);
            }

            if (!"true".equalsIgnoreCase(writeConcernFsyncValue) && !"false".equalsIgnoreCase(writeConcernFsyncValue)) {
                System.err.println("ERROR: Invalid writeConcern.fsync: '" + writeConcernFsyncValue + "'. " +
                                   "Must be true or false");
                System.exit(1);
            };

            if (!"true".equalsIgnoreCase(writeConcernJValue) && !"false".equalsIgnoreCase(writeConcernJValue)) {
                System.err.println("ERROR: Invalid writeConcern.j: '" + writeConcernJValue + "'. " +
                                   "Must be true or false");
                System.exit(1);
            }

            if (!"true".equalsIgnoreCase(writeConcernContinueValue) && !"false".equalsIgnoreCase(writeConcernContinueValue)) {
                System.err.println("ERROR: Invalid writeConcern.continueOnErrorForInsert: '" + writeConcernContinueValue + "'. " +
                                   "Must be true or false");
                System.exit(1);
            }

            writeConcern = new WriteConcern(writeConcernWInt,
                                            writeTimeoutInt,
                                            Boolean.parseBoolean(writeConcernFsyncValue),
                                            Boolean.parseBoolean(writeConcernJValue),
                                            Boolean.parseBoolean(writeConcernContinueValue));
        }
        else {
            writeConcern = WriteConcern.valueOf(writeConcernValue);
        }

        readPreference = ReadPreference.valueOf(readPreferenceValue);

        //TODO: support tagset

        try {
            // strip out prefix since Java driver doesn't currently support
            // standard connection format URL yet
            // http://www.mongodb.org/display/DOCS/Connections
            if (url.startsWith("mongodb://")) {
                url = url.substring(10);
            }

            // need to append db to url.
            url += "/"+database;
            System.out.println("new database url = "+url);
            mongo = new Mongo(new DBAddress(url));
            System.out.println("mongo connection created with "+url);
        } catch (Exception e1) {
            System.err.println(
                    "Could not initialize MongoDB connection pool for Loader: "
                            + e1.toString());
            e1.printStackTrace();
            return;
        }

    }
    
    @Override
	/**
	 * Cleanup any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
	public void cleanup() throws DBException
	{
        try {
        	mongo.close();
        } catch (Exception e1) {
        	System.err.println(
                    "Could not close MongoDB connection pool: "
                            + e1.toString());
            e1.printStackTrace();
            return;
        }
	}

    @Override
    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    public int delete(String table, String key) {
        com.mongodb.DB db=null;
        try {
            db = mongo.getDB(database);
            db.requestStart();
            DBCollection collection = db.getCollection(table);
            DBObject q = new BasicDBObject().append("_id", key);
            WriteResult res = collection.remove(q, writeConcern);
            return res.getN() == 1 ? 0 : 1;
        } catch (Exception e) {
            e.printStackTrace();
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
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        com.mongodb.DB db = null;
        try {
            db = mongo.getDB(database);

            db.requestStart();

            DBCollection collection = db.getCollection(table);
            DBObject r = new BasicDBObject().append("_id", key);
	        for(String k: values.keySet()) {
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
        } catch (Exception e) {
            e.printStackTrace();
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
     * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    public int read(String table, String key, Set<String> fields,
            HashMap<String, ByteIterator> result) {
        com.mongodb.DB db = null;
        try {
            db = mongo.getDB(database);

            db.requestStart();

            DBCollection collection = db.getCollection(table);
            DBObject q = new BasicDBObject().append("_id", key);
            DBObject fieldsToReturn = new BasicDBObject();
            boolean returnAllFields = fields == null;

            DBObject queryResult = null;
            if (!returnAllFields) {
                Iterator<String> iter = fields.iterator();
                while (iter.hasNext()) {
                    fieldsToReturn.put(iter.next(), 1);
                }
                queryResult = collection.findOne(q, fieldsToReturn, readPreference);
            } else {
                queryResult = collection.findOne(q, null, readPreference);
            }

            if (queryResult != null) {
                result.putAll(resultify(queryResult));
            }
            return queryResult != null ? 0 : 1;
        } catch (Exception e) {
            e.printStackTrace();
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
     * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     * @param table The name of the table
     * @param key The record key of the record to write.
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
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
        } catch (Exception e) {
            e.printStackTrace();
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
     * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param startkey The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param fields The list of fields to read, or null for all of them
     * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    public int scan(String table, String startkey, int recordcount,
            Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        com.mongodb.DB db=null;
        try {
            db = mongo.getDB(database);
            db.requestStart();
            DBCollection collection = db.getCollection(table);
            // { "_id":{"$gte":startKey, "$lte":{"appId":key+"\uFFFF"}} }
            DBObject scanRange = new BasicDBObject().append("$gte", startkey);
            DBObject q = new BasicDBObject().append("_id", scanRange);
            DBCursor cursor = collection.find(q).limit(recordcount);    //TODO: apply readPreference here
            while (cursor.hasNext()) {
                result.add(resultify(cursor.next()));
            }

            return 0;
        } catch (Exception e) {
            e.printStackTrace();
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

