/**
 * Copyright (c) 2012 - 2016 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.yahoo.ycsb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * OrientDB client for YCSB framework.
 * <p>
 * Properties to set:
 * <p>
 * orientdb.url=local:C:/temp/databases or remote:localhost:2424 <br>
 * orientdb.database=ycsb <br>
 * orientdb.user=admin <br>
 * orientdb.password=admin <br>
 *
 * @author Luca Garulli
 * @author Andrey Lomakin
 */
public class OrientDBClient extends DB {
  private static final String URL_PROPERTY         = "orientdb.url";
  private static final String URL_PROPERTY_DEFAULT =
      "plocal:." + File.separator + "target" + File.separator + "databases" + File.separator + "ycsb";

  private static final String USER_PROPERTY         = "orientdb.user";
  private static final String USER_PROPERTY_DEFAULT = "admin";

  private static final String PASSWORD_PROPERTY         = "orientdb.password";
  private static final String PASSWORD_PROPERTY_DEFAULT = "admin";

  private static final String NEWDB_PROPERTY         = "orientdb.newdb";
  private static final String NEWDB_PROPERTY_DEFAULT = "false";

  private static final String STORAGE_TYPE_PROPERTY = "orientdb.remote.storagetype";

  private static final String ORIENTDB_DOCUMENT_TYPE = "document";

  private static final String CLASS = "usertable";

  private static final Lock    INIT_LOCK = new ReentrantLock();
  private static       boolean dbChecked = false;
  private static volatile OPartitionedDatabasePool databasePool;
  private static boolean initialized   = false;
  private static int     clientCounter = 0;

  private boolean isRemote = false;

  private static final Logger LOG = LoggerFactory.getLogger(OrientDBClient.class);

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one DB instance per client thread.
   */
  public void init() throws DBException {
    // initialize OrientDB driver
    final Properties props = getProperties();
    String url = props.getProperty(URL_PROPERTY, URL_PROPERTY_DEFAULT);
    String user = props.getProperty(USER_PROPERTY, USER_PROPERTY_DEFAULT);

    String password = props.getProperty(PASSWORD_PROPERTY, PASSWORD_PROPERTY_DEFAULT);
    Boolean newdb = Boolean.parseBoolean(props.getProperty(NEWDB_PROPERTY, NEWDB_PROPERTY_DEFAULT));
    String remoteStorageType = props.getProperty(STORAGE_TYPE_PROPERTY);

    INIT_LOCK.lock();
    try {
      clientCounter++;
      if (!initialized) {
        OGlobalConfiguration.dumpConfiguration(System.out);

        LOG.info("OrientDB loading database url = " + url);

        ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);

        if (db.getStorage().isRemote()) {
          isRemote = true;
        }

        if (!dbChecked) {
          if (!isRemote) {
            if (newdb) {
              if (db.exists()) {
                db.open(user, password);
                LOG.info("OrientDB drop and recreate fresh db");

                db.drop();
              }

              db.create();
            } else {
              if (!db.exists()) {
                LOG.info("OrientDB database not found, creating fresh db");

                db.create();
              }
            }
          } else {
            OServerAdmin server = new OServerAdmin(url).connect(user, password);

            if (remoteStorageType == null) {
              throw new DBException(
                  "When connecting to a remote OrientDB instance, "
                      + "specify a database storage type (plocal or memory) with "
                      + STORAGE_TYPE_PROPERTY);
            }

            if (newdb) {
              if (server.existsDatabase()) {
                LOG.info("OrientDB drop and recreate fresh db");

                server.dropDatabase(remoteStorageType);
              }

              server.createDatabase(db.getName(), ORIENTDB_DOCUMENT_TYPE, remoteStorageType);
            } else {
              if (!server.existsDatabase()) {

                LOG.info("OrientDB database not found, creating fresh db");
                server.createDatabase(server.getURL(), ORIENTDB_DOCUMENT_TYPE, remoteStorageType);
              }
            }

            server.close();
          }

          dbChecked = true;
        }

        if (db.isClosed()) {
          db.open(user, password);
        }

        if (!db.getMetadata().getSchema().existsClass(CLASS)) {
          db.getMetadata().getSchema().createClass(CLASS);
        }

        db.close();

        if (databasePool == null) {
          databasePool = new OPartitionedDatabasePool(url, user, password);
        }

        initialized = true;
      }
    } catch (Exception e) {
      LOG.error("Could not initialize OrientDB connection pool for Loader: " + e.toString());
      e.printStackTrace();
    } finally {
      INIT_LOCK.unlock();
    }

  }

  OPartitionedDatabasePool getDatabasePool() {
    return databasePool;
  }

  @Override
  public void cleanup() throws DBException {
    INIT_LOCK.lock();
    try {
      clientCounter--;
      if (clientCounter == 0) {
        databasePool.close();
      }

      databasePool = null;
      initialized = false;
    } finally {
      INIT_LOCK.unlock();
    }

  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified values
   * HashMap will be written into the record with the specified
   * record key.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error.
   * See this class's description for a discussion of error codes.
   */
  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    try (ODatabaseDocumentTx db = databasePool.acquire()) {
      final ODocument document = new ODocument(CLASS);

      for (Map.Entry<String, String> entry : StringByteIterator.getStringMap(values).entrySet()) {
        document.field(entry.getKey(), entry.getValue());
      }

      document.save();
      final ODictionary<ORecord> dictionary = db.getMetadata().getIndexManager().getDictionary();
      dictionary.put(key, document);

      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key   The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error.
   * See this class's description for a discussion of error codes.
   */
  @Override
  public Status delete(String table, String key) {
    while (true) {
      try (ODatabaseDocumentTx db = databasePool.acquire()) {
        final ODictionary<ORecord> dictionary = db.getMetadata().getIndexManager().getDictionary();
        dictionary.remove(key);
        return Status.OK;
      } catch (OConcurrentModificationException cme) {
        continue;
      } catch (Exception e) {
        e.printStackTrace();
        return Status.ERROR;
      }
    }
  }

  /**
   * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error or "not found".
   */
  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    try (ODatabaseDocumentTx db = databasePool.acquire()) {
      final ODictionary<ORecord> dictionary = db.getMetadata().getIndexManager().getDictionary();
      final ODocument document = dictionary.get(key);
      if (document != null) {
        if (fields != null) {
          for (String field : fields) {
            result.put(field, new StringByteIterator((String) document.field(field)));
          }
        } else {
          for (String field : document.fieldNames()) {
            result.put(field, new StringByteIterator((String) document.field(field)));
          }
        }
        return Status.OK;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified values
   * HashMap will be written into the record with the specified
   * record key, overwriting any existing values with the same field name.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error. See this class's description f
   * or a discussion of error codes.
   */
  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    while (true) {
      try (ODatabaseDocumentTx db = databasePool.acquire()) {
        final ODictionary<ORecord> dictionary = db.getMetadata().getIndexManager().getDictionary();
        final ODocument document = dictionary.get(key);
        if (document != null) {
          for (Map.Entry<String, String> entry : StringByteIterator.getStringMap(values).entrySet()) {
            document.field(entry.getKey(), entry.getValue());
          }

          document.save();
          return Status.OK;
        }
      } catch (OConcurrentModificationException cme) {
        continue;
      } catch (Exception e) {
        e.printStackTrace();
        return Status.ERROR;
      }
    }
  }

  /**
   * Perform a range scan for a set of records in the database.
   * Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table       The name of the table
   * @param startkey    The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields      The list of fields to read, or null for all of them
   * @param result      A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
   * @return Zero on success, a non-zero error code on error.
   * See this class's description for a discussion of error codes.
   */
  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {

    if (isRemote) {
      // Iterator methods needed for scanning are Unsupported for remote database connections.
      LOG.warn("OrientDB scan operation is not implemented for remote database connections.");
      return Status.NOT_IMPLEMENTED;
    }

    try (ODatabaseDocumentTx db = databasePool.acquire()) {
      final ODictionary<ORecord> dictionary = db.getMetadata().getIndexManager().getDictionary();
      final OIndexCursor entries = dictionary.getIndex().iterateEntriesMajor(startkey, true, true);

      int currentCount = 0;
      while (entries.hasNext()) {
        final ODocument document = entries.next().getRecord();

        final HashMap<String, ByteIterator> map = new HashMap<>();
        result.add(map);

        if (fields != null) {
          for (String field : fields) {
            map.put(field, new StringByteIterator((String) document.field(field)));
          }
        } else {
          for (String field : document.fieldNames()) {
            map.put(field, new StringByteIterator((String) document.field(field)));
          }
        }

        currentCount++;

        if (currentCount >= recordcount) {
          break;
        }
      }

      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }
}
