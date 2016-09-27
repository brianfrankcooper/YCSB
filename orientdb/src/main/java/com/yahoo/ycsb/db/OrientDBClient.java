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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.client.remote.OEngineRemote;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.intent.OIntentMassiveRead;
import com.orientechnologies.orient.core.intent.OIntentNoCache;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * OrientDB client for YCSB framework.
 */
public class OrientDBClient extends DB {

  private static final String CLASS = "usertable";
  private ODatabaseDocumentTx db;
  private ODictionary<ORecord> dictionary;
  private boolean isRemote = false;

  private static final String URL_PROPERTY = "orientdb.url";

  private static final String USER_PROPERTY = "orientdb.user";
  private static final String USER_PROPERTY_DEFAULT = "admin";

  private static final String PASSWORD_PROPERTY = "orientdb.password";
  private static final String PASSWORD_PROPERTY_DEFAULT = "admin";

  private static final String NEWDB_PROPERTY = "orientdb.newdb";
  private static final String NEWDB_PROPERTY_DEFAULT = "false";

  private static final String STORAGE_TYPE_PROPERTY = "orientdb.remote.storagetype";

  private static final String INTENT_PROPERTY = "orientdb.intent";
  private static final String INTENT_PROPERTY_DEFAULT = "";

  private static final String DO_TRANSACTIONS_PROPERTY = "dotransactions";
  private static final String DO_TRANSACTIONS_PROPERTY_DEFAULT = "true";

  private static final String ORIENTDB_DOCUMENT_TYPE = "document";
  private static final String ORIENTDB_MASSIVEINSERT = "massiveinsert";
  private static final String ORIENTDB_MASSIVEREAD = "massiveread";
  private static final String ORIENTDB_NOCACHE = "nocache";

  private static final Logger LOG = LoggerFactory.getLogger(OrientDBClient.class);

  /**
   * This method abstracts the administration of OrientDB namely creating and connecting to a database.
   * Creating a database needs to be done in a synchronized method so that multiple threads do not all try
   * to run the creation operation simultaneously, this ends in failure.
   *
   * @param props Workload properties object
   * @return a usable ODatabaseDocumentTx object
   * @throws DBException
   */
  private static synchronized ODatabaseDocumentTx initDB(Properties props) throws DBException {
    String url = props.getProperty(URL_PROPERTY);
    String user = props.getProperty(USER_PROPERTY, USER_PROPERTY_DEFAULT);
    String password = props.getProperty(PASSWORD_PROPERTY, PASSWORD_PROPERTY_DEFAULT);
    Boolean newdb = Boolean.parseBoolean(props.getProperty(NEWDB_PROPERTY, NEWDB_PROPERTY_DEFAULT));
    String remoteStorageType = props.getProperty(STORAGE_TYPE_PROPERTY);
    Boolean isrun = Boolean.parseBoolean(props.getProperty(DO_TRANSACTIONS_PROPERTY, DO_TRANSACTIONS_PROPERTY_DEFAULT));

    ODatabaseDocumentTx dbconn;

    if (url == null) {
      throw new DBException(String.format("Required property \"%s\" missing for OrientDBClient", URL_PROPERTY));
    }

    LOG.info("OrientDB loading database url = " + url);

    // If using a remote database, use the OServerAdmin interface to connect
    if (url.startsWith(OEngineRemote.NAME)) {
      if (remoteStorageType == null) {
        throw new DBException("When connecting to a remote OrientDB instance, " +
          "specify a database storage type (plocal or memory) with " + STORAGE_TYPE_PROPERTY);
      }

      try {
        OServerAdmin server = new OServerAdmin(url).connect(user, password);

        if (server.existsDatabase()) {
          if (newdb && !isrun) {
            LOG.info("OrientDB dropping and recreating fresh db on remote server.");
            server.dropDatabase(remoteStorageType);
            server.createDatabase(server.getURL(), ORIENTDB_DOCUMENT_TYPE, remoteStorageType);
          }
        } else {
          LOG.info("OrientDB database not found, creating fresh db");
          server.createDatabase(server.getURL(), ORIENTDB_DOCUMENT_TYPE, remoteStorageType);
        }

        server.close();
        dbconn = new ODatabaseDocumentTx(url).open(user, password);
      } catch (IOException | OException e) {
        throw new DBException(String.format("Error interfacing with %s", url), e);
      }
    } else {
      try {
        dbconn = new ODatabaseDocumentTx(url);
        if (dbconn.exists()) {
          dbconn.open(user, password);
          if (newdb && !isrun) {
            LOG.info("OrientDB dropping and recreating fresh db.");
            dbconn.drop();
            dbconn.create();
          }
        } else {
          LOG.info("OrientDB database not found, creating fresh db");
          dbconn.create();
        }
      } catch (ODatabaseException e) {
        throw new DBException(String.format("Error interfacing with %s", url), e);
      }
    }

    if (dbconn == null) {
      throw new DBException("Could not establish connection to: " + url);
    }

    LOG.info("OrientDB connection created with " + url);
    return dbconn;
  }

  @Override
  public void init() throws DBException {
    Properties props = getProperties();

    String intent = props.getProperty(INTENT_PROPERTY, INTENT_PROPERTY_DEFAULT);

    db = initDB(props);

    if (db.getURL().startsWith(OEngineRemote.NAME)) {
      isRemote = true;
    }

    dictionary = db.getMetadata().getIndexManager().getDictionary();
    if (!db.getMetadata().getSchema().existsClass(CLASS)) {
      db.getMetadata().getSchema().createClass(CLASS);
    }

    if (intent.equals(ORIENTDB_MASSIVEINSERT)) {
      LOG.info("Declaring intent of MassiveInsert.");
      db.declareIntent(new OIntentMassiveInsert());
    } else if (intent.equals(ORIENTDB_MASSIVEREAD)) {
      LOG.info("Declaring intent of MassiveRead.");
      db.declareIntent(new OIntentMassiveRead());
    } else if (intent.equals(ORIENTDB_NOCACHE)) {
      LOG.info("Declaring intent of NoCache.");
      db.declareIntent(new OIntentNoCache());
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
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    try {
      final ODocument document = new ODocument(CLASS);
      for (Entry<String, String> entry : StringByteIterator.getStringMap(values).entrySet()) {
        document.field(entry.getKey(), entry.getValue());
      }
      document.save();
      dictionary.put(key, document);

      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
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
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    try {
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

  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    try {
      final ODocument document = dictionary.get(key);
      if (document != null) {
        for (Entry<String, String> entry : StringByteIterator.getStringMap(values).entrySet()) {
          document.field(entry.getKey(), entry.getValue());
        }
        document.save();
        return Status.OK;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    if (isRemote) {
      // Iterator methods needed for scanning are Unsupported for remote database connections.
      LOG.warn("OrientDB scan operation is not implemented for remote database connections.");
      return Status.NOT_IMPLEMENTED;
    }

    try {
      int entrycount = 0;
      final OIndexCursor entries = dictionary.getIndex().iterateEntriesMajor(startkey, true, true);

      while (entries.hasNext() && entrycount < recordcount) {
        final OIdentifiable entry = entries.next();
        final ODocument document = entry.getRecord();

        final HashMap<String, ByteIterator> map = new HashMap<String, ByteIterator>();
        result.add(map);

        if (fields != null && !fields.isEmpty()) {
          for (String field : fields) {
            map.put(field, new StringByteIterator((String) document.field(field)));
          }
        }

        entrycount++;
      }

      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  /**
   * Access method to db variable for unit testing.
   **/
  ODatabaseDocumentTx getDB() {
    return db;
  }
}
