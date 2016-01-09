/**
 * Copyright (c) 2012 - 2016 YCSB contributors. All rights reserved.
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

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * OrientDB client for YCSB framework.
 */
public class OrientDBClient extends DB {

  private static final String             CLASS = "usertable";
  protected ODatabaseDocumentTx             db;
  private ODictionary<ORecord> dictionary;

  private static final String URL_PROPERTY = "orientdb.url";

  private static final String USER_PROPERTY = "orientdb.user";
  private static final String USER_PROPERTY_DEFAULT = "admin";

  private static final String PASSWORD_PROPERTY = "orientdb.password";
  private static final String PASSWORD_PROPERTY_DEFAULT = "admin";

  private static final String NEWDB_PROPERTY = "orientdb.newdb";
  private static final String NEWDB_PROPERTY_DEFAULT = "false";

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one DB instance per client thread.
   */
  public void init() throws DBException {
    Properties props = getProperties();

    String url = props.getProperty(URL_PROPERTY);
    String user = props.getProperty(USER_PROPERTY, USER_PROPERTY_DEFAULT);
    String password = props.getProperty(PASSWORD_PROPERTY, PASSWORD_PROPERTY_DEFAULT);
    Boolean newdb = Boolean.parseBoolean(props.getProperty(NEWDB_PROPERTY, NEWDB_PROPERTY_DEFAULT));

    if (url == null) {
      throw new DBException(String.format("Required property \"%s\" missing for OrientDBClient", URL_PROPERTY));
    }

    try {
      System.out.println("OrientDB loading database url = " + url);

      OGlobalConfiguration.STORAGE_KEEP_OPEN.setValue(false);
      db = new ODatabaseDocumentTx(url);
      if (db.exists()) {
        db.open(user, password);
        if (newdb) {
          System.out.println("OrientDB drop and recreate fresh db");
          db.drop();
          db.create();
        }
      } else {
        System.out.println("OrientDB database not found, create fresh db");
        db.create();
      }

      System.out.println("OrientDB connection created with " + url);

      dictionary = db.getMetadata().getIndexManager().getDictionary();
      if (!db.getMetadata().getSchema().existsClass(CLASS))
        db.getMetadata().getSchema().createClass(CLASS);

      db.declareIntent(new OIntentMassiveInsert());

    } catch (Exception e1) {
      System.err.println("Could not initialize OrientDB connection pool for Loader: " + e1.toString());
      e1.printStackTrace();
      return;
    }

  }

  @Override
  public void cleanup() throws DBException {
    // Set this thread's db reference (needed for thread safety in testing)
    ODatabaseRecordThreadLocal.INSTANCE.set(db);

    if (db != null) {
      db.close();
      db = null;
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
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    try {
      final ODocument document = new ODocument(CLASS);
      for (Entry<String, String> entry : StringByteIterator.getStringMap(values).entrySet())
        document.field(entry.getKey(), entry.getValue());
      document.save();
      dictionary.put(key, document);

      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
   */
  public Status delete(String table, String key) {
    try {
      dictionary.remove(key);
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
  /**
   * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table The name of the table
   * @param key The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error or "not found".
   */
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    try {
      final ODocument document = dictionary.get(key);
      if (document != null) {
        if (fields != null)
          for (String field : fields)
            result.put(field, new StringByteIterator((String) document.field(field)));
        else
          for (String field : document.fieldNames())
            result.put(field, new StringByteIterator((String) document.field(field)));
        return Status.OK;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
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
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    try {
      final ODocument document = dictionary.get(key);
      if (document != null) {
        for (Entry<String, String> entry : StringByteIterator.getStringMap(values).entrySet())
          document.field(entry.getKey(), entry.getValue());
        document.save();
        return Status.OK;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
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
  public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    try {
      int entrycount = 0;
      final OIndexCursor entries = dictionary.getIndex().iterateEntriesMajor(startkey, true, true);

      while (entries.hasNext() && entrycount < recordcount) {
        final Entry<Object, OIdentifiable> entry = entries.nextEntry();
        final ODocument document = entry.getValue().getRecord();

        final HashMap<String, ByteIterator> map = new HashMap<String, ByteIterator>();
        result.add(map);

        for (String field : fields) {
          map.put(field, new StringByteIterator((String) document.field(field)));
        }

        entrycount++;
      }

      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }
}
