/**
 * MongoDB client binding for YCSB.
 *
 * Submitted by Yen Pai on 5/11/2010.
 *
 * https://gist.github.com/000a66b8db2caf42467b#file_mongo_db.java
 *
 */

package com.yahoo.ycsb.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.Map;
import java.util.Vector;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DBAddress;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

/**
 * MongoDB client for YCSB framework.
 *
 * Properties to set:
 *
 * mongodb.url=mongodb://localhost:27017
 * mongodb.database=ycsb
 * mongodb.writeConcern=normal
 *
 * @author ypai
 *
 */
public class MongoDbClient extends DB {

    private Mongo mongo;
    private WriteConcern writeConcern;
    private String database;

    /**
     * Initialize any state for this DB. Called once per DB instance; there is
     * one DB instance per client thread.
     */
    public void init() throws DBException {
        // initialize MongoDb driver
        Properties props = getProperties();
        String url = props.getProperty("mongodb.url");
        database = props.getProperty("mongodb.database");
        String writeConcernType = props.getProperty("mongodb.writeConcern");

        if ("none".equals(writeConcernType)) {
            writeConcern = WriteConcern.NONE;
        } else if ("strict".equals(writeConcernType)) {
            writeConcern = WriteConcern.SAFE;
        } else if ("normal".equals(writeConcernType)) {
            writeConcern = WriteConcern.NORMAL;
        }

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
            WriteResult res = collection.insert(r,writeConcern);
            return res.getError() == null ? 0 : 1;
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
                queryResult = collection.findOne(q, fieldsToReturn);
            } else {
                queryResult = collection.findOne(q);
            }

            if (queryResult != null) {
                result.putAll(queryResult.toMap());
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
            return res.getN() == 1 ? 0 : 1;
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
            DBCursor cursor = collection.find(q).limit(recordcount);
            while (cursor.hasNext()) {
                //toMap() returns a Map, but result.add() expects a Map<String,String>. Hence, the suppress warnings.
                result.add(StringByteIterator.getByteIteratorMap((Map<String,String>)cursor.next().toMap()));
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
}

