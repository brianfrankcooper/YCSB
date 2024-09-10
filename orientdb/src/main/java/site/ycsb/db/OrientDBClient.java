/**
 * Copyright (c) 2012 - 2021 YCSB contributors. All rights reserved.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package site.ycsb.db;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.util.OURLConnection;
import com.orientechnologies.orient.core.util.OURLHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.*;

import java.io.File;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/** OrientDB client for YCSB framework. */
public class OrientDBClient extends DB {
  private static final Logger LOG = LoggerFactory.getLogger(OrientDBClient.class);

  private static final String URL_PROPERTY = "orientdb.url";
  private static final String URL_PROPERTY_DEFAULT = "remote:localhost" + File.separator + "ycsb";

  private static final String USER_PROPERTY = "orientdb.user";
  private static final String USER_PROPERTY_DEFAULT = "admin";

  private static final String PASSWORD_PROPERTY = "orientdb.password";
  private static final String PASSWORD_PROPERTY_DEFAULT = "admin";

  private static final String SERVER_USER_PROPERTY = "orientdb.server.user";
  private static final String SERVER_USER_PROPERTY_DEFAULT = "root";

  private static final String SERVER_PASSWORD_PROPERTY = "orientdb.server.password";
  private static final String SERVER_PASSWORD_PROPERTY_DEFAULT = "admin";

  private static final String NEWDB_PROPERTY = "orientdb.newdb";
  private static final String NEWDB_PROPERTY_DEFAULT = "false";

  private static final String STORAGE_TYPE_PROPERTY = "orientdb.remote.storagetype";

  private static final String CLASS = "usertable";

  private static final Lock REENTRANT_INIT_LOCK = new ReentrantLock();
  private static volatile ODatabasePool pool;
  private static volatile OrientDB orient;

  private static boolean initialized = false;
  private static int clientCounter = 0;

  /** The batch size to use for inserts. */
  private static int batchSize = 1;

  private final List<OElement> elementsBatch = new ArrayList<>();

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one DB instance per
   * client thread.
   */
  public void init() throws DBException {
    batchSize = Integer.parseInt(getProperties().getProperty("batchsize", "1"));

    REENTRANT_INIT_LOCK.lock();
    try {
      clientCounter++;
      if (!initialized) {
        final ConnectionProperties cp = new ConnectionProperties();

        String url = cp.getUrl();
        LOG.info("OrientDB loading database url = " + url);
        final OURLConnection urlHelper = OURLHelper.parseNew(url);
        initAndGetDatabaseUrlAndDbType(url, cp.getServerUser(), cp.getServerUserPassword());

        final String dbName = urlHelper.getDbName();
        if (cp.newDb && orient.exists(dbName)) {
          orient.drop(dbName);
        }
        ODatabaseType dbType = urlHelper.getDbType().orElse(ODatabaseType.PLOCAL);
        orient.createIfNotExists(dbName, dbType);
        if (!orient.isOpen()) {
          orient.open(dbName, cp.getUser(), cp.getPassword());
        }
        if (pool == null) {
          pool = new ODatabasePool(orient, dbName, cp.getUser(), cp.getPassword());
        }
        try (final ODatabaseSession session = pool.acquire()) {
          final OClass newClass = session.createClassIfNotExist(CLASS);
          LOG.info("OrientDB class created = " + CLASS);
          newClass.createProperty("key", OType.STRING);
          LOG.info("OrientDB class property created = 'key'.");
        }
        createIndexForCollection(dbName);
        initialized = true;
        LOG.info("OrientDB successfully initialized.");
      }
    } catch (final Exception e) {
      LOG.error("Could not initialize OrientDB connection pool for Loader: " + e.toString());
      e.printStackTrace();
    } finally {
      REENTRANT_INIT_LOCK.unlock();
    }
  }

  private void initAndGetDatabaseUrlAndDbType(String url, final String serverUser, final String serverPassword) {
    if (url.startsWith("remote:")) {
      final OrientDBConfigBuilder poolCfg = OrientDBConfig.builder();
      poolCfg.addConfig(OGlobalConfiguration.DB_POOL_MIN, 5);
      poolCfg.addConfig(OGlobalConfiguration.DB_POOL_MAX, 10);
      final OrientDBConfig oriendDBconfig = poolCfg.build();
      if (orient == null) {
        orient = new OrientDB(url, serverUser, serverPassword, oriendDBconfig);
      }
    } else if (url.startsWith("memory:")) {
      url = "embedded:";
      if (orient == null) {
        orient = new OrientDB(url, OrientDBConfig.defaultConfig());
      }
    } else {
      if (orient == null) {
        orient = new OrientDB(url, OrientDBConfig.defaultConfig());
      }
    }
  }

  private void createIndexForCollection(final String dbTable) {
    try (final ODatabaseSession session = pool.acquire()) {
      final OClass cls = session.getClass(CLASS);
      cls.createIndex(dbTable + "keyidx", OClass.INDEX_TYPE.NOTUNIQUE, "key");
      LOG.info(
          "OrientDB index created = 'keyidx' of type = "
              + OClass.INDEX_TYPE.NOTUNIQUE
              + " on 'key'.");
    }
  }

  protected ODatabasePool getDatabasePool() {
    return pool;
  }

  @Override
  public void cleanup() throws DBException {
    REENTRANT_INIT_LOCK.lock();
    try {
      clientCounter--;
      if (clientCounter == 0) {
        orient.close();
        orient = null;
        pool.close();
        pool = null;
        initialized = false;
        LOG.info("OrientDB successful cleanup.");
      }
    } finally {
      REENTRANT_INIT_LOCK.unlock();
    }
  }

  public Status flush(final String table) throws DBException {
    try (final ODatabaseSession session = pool.acquire()) {
      session.begin();
      // actually Status.NOTHING_TO_DO
      return (elementsBatch.size() == 0) ? Status.OK : commitBatch(session);
    }
  }

  void dropTable(final String dbName) {
    if (orient != null) {
      orient.drop(dbName);
    }
  }

  @Override
  public Status insert(
      final String table, final String key, final Map<String, ByteIterator> values) {
    try (final ODatabaseSession session = pool.acquire()) {
      // create outside of tx
      final OElement element = session.newInstance(table);
      element.setProperty("key", key);

      session.begin();
      StringByteIterator.getStringMap(values).entrySet().stream()
          .forEach(e -> element.setProperty(e.getKey(), e.getValue()));
      if (batchSize == 1) {
        element.save();
        session.commit();
        return Status.OK;
      } else {
        elementsBatch.add(element);
      }
      return (elementsBatch.size() != batchSize) ? Status.BATCHED_OK : commitBatch(session);
    } catch (final Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  private Status commitBatch(final ODatabaseSession session) {
    try {
      for (final OElement element : elementsBatch) {
        element.save();
      }
      session.commit();
    } catch (final Exception e) {
      System.err.println("Unable to insert batch data n. " + elementsBatch.size());
      return Status.ERROR;
    } finally {
      elementsBatch.clear();
    }
    return Status.OK;
  }

  @Override
  public Status delete(final String table, final String key) {
    while (true) {
      try (final ODatabaseSession session = pool.acquire()) {
        session.begin();
        final Map<String, Object> params = new HashMap<>();
        params.put("key", key);
        final String delete = "DELETE FROM " + table + " WHERE key = :key";
        session.command(delete, params);
        session.commit();
        return Status.OK;
      } catch (OConcurrentModificationException cme) {
        // just continue
      } catch (final Exception e) {
        e.printStackTrace();
        return Status.ERROR;
      }
    }
  }

  @Override
  public Status read(
      final String table,
      final String key,
      final Set<String> fields,
      final Map<String, ByteIterator> result) {
    return fields == null
        ? readAllValues(table, key, result)
        : readSelectedFields(table, key, fields, result);
  }

  private Status readSelectedFields(
      final String table,
      final String key,
      final Set<String> fields,
      final Map<String, ByteIterator> result) {
    final Map<String, Object> params = new HashMap<>();
    params.put("key", key);
    final String querySelected =
        "SELECT "
            + fields.stream().collect(Collectors.joining(", "))
            + " FROM `"
            + table
            + "` "
            + "WHERE key = :key";
    try (final ODatabaseSession session = pool.acquire();
        final OResultSet rs = session.query(querySelected, params)) {
      rs.stream()
          .forEach(
              e -> e.getPropertyNames().stream()
                  .forEach(
                      property ->
                          result.put(property, new StringByteIterator(e.getProperty(property)))));
    } catch (final Exception e) {
      System.err.println("Unable to read data for key " + key);
      return Status.ERROR;
    }
    return Status.OK;
  }

  private Status readAllValues(
      final String table, final String key, final Map<String, ByteIterator> result) {
    final Map<String, Object> params = new HashMap<>();
    params.put("key", key);
    final String queryAll = "SELECT * " + "FROM `" + table + "` " + "WHERE key = :key";
    try (final ODatabaseSession session = pool.acquire();
        final OResultSet rs = session.query(queryAll, params)) {
      rs.stream()
          .forEach(
              e -> e.getPropertyNames().stream()
                  .forEach(
                      property ->
                          result.put(property, new StringByteIterator(e.getProperty(property)))));
    } catch (final Exception e) {
      System.err.println("Unable to read data for key " + key);
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status scan(
      final String table,
      final String startkey,
      final int recordcount,
      final Set<String> fields,
      final Vector<HashMap<String, ByteIterator>> result) {

    final Map<String, Object> params = new HashMap<>();
    params.put("key", startkey);
    params.put("limit", recordcount);
    // TODO: map to parameters OR iterate in the try block and select *
    final String scan = fields == null || fields.isEmpty() ?
        "SELECT *"
            + " FROM `"
            + table
            + "` "
            + "WHERE key >= ':key'"
            + " ORDER BY key ASC"
            + " LIMIT :limit" :
        "SELECT "
            + fields.stream().collect(Collectors.joining(", "))
            + " FROM `"
            + table
            + "` "
            + "WHERE key >= ':key'"
            + " ORDER BY key ASC"
            + " LIMIT :limit";
    try (final ODatabaseSession session = pool.acquire();
        final OResultSet rs = session.query(scan, params)) {
      rs.stream()
          .forEach(
              e -> {
              final HashMap<String, ByteIterator> entry = new HashMap<>();
              e.getPropertyNames().stream()
                  .forEach(
                      property -> entry.put(property, new StringByteIterator(e.getProperty(property))));
              result.addElement(entry);
            });
    } catch (final Exception e) {
      System.err.println("Unable to read data for key " + startkey);
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status update(
      final String table, final String key, final Map<String, ByteIterator> values) {
    while (true) {
      try (final ODatabaseSession session = pool.acquire()) {
        session.begin();
        final Map<String, Object> params = new HashMap<>();
        params.put("key", escapeUnsupportedChars(key));
        final String update = preparedUpdateSql(table, values);
        session.command(update, params);
        session.commit();
        return Status.OK;
      } catch (OConcurrentModificationException cme) {
        continue;
      } catch (Exception e) {
        e.printStackTrace();
        return Status.ERROR;
      }
    }
  }

  private String preparedUpdateSql(final String table, final Map<String, ByteIterator> values) {
    return "UPDATE "
        + table
        + " SET "
        + values.entrySet().stream()
            .map(e -> " " + escapeUnsupportedChars(e.getKey()) + "= '" + escapeUnsupportedChars(e.getValue().toString())
                + "'").collect(Collectors.joining(", "))
        + " WHERE key = :key";
  }

  /**
   * OrientDB does not allow certain characters in keys like quotes etc.
   * @param key
   * @return
   */
  private String escapeUnsupportedChars(final String key) {
    // Brutal
    return key.replace("\"", "").replace("'", "").replace("\\", "");
  }

  protected class ConnectionProperties {
    private final String url;
    private final String user;
    private final String password;

    private final String serverUser;
    private final String serverUserPassword;
    private final boolean newDb;

    public ConnectionProperties() {
      final Properties props = getProperties();
      url = props.getProperty(URL_PROPERTY, URL_PROPERTY_DEFAULT);
      user = props.getProperty(USER_PROPERTY, USER_PROPERTY_DEFAULT);
      password = props.getProperty(PASSWORD_PROPERTY, PASSWORD_PROPERTY_DEFAULT);
      serverUser = props.getProperty(SERVER_USER_PROPERTY, SERVER_PASSWORD_PROPERTY_DEFAULT);
      serverUserPassword = props.getProperty(SERVER_PASSWORD_PROPERTY, SERVER_PASSWORD_PROPERTY_DEFAULT);
      newDb = Boolean.parseBoolean(props.getProperty(NEWDB_PROPERTY, NEWDB_PROPERTY_DEFAULT));
      final String remoteStorageType = props.getProperty(STORAGE_TYPE_PROPERTY);

      LOG.info(
          "\nProperties: \n\turl="
              + url
              + "\n\t"
              + "user="
              + user
              + "\n\t"
              + "user(server)="
              + serverUser
              + "\n\t"
              + "isNewDb="
              + newDb
              + "\n\t"
              + "remoteStorageType="
              + remoteStorageType);
    }

    String getUrl() {
      return url;
    }

    String getUser() {
      return user;
    }

    String getPassword() {
      return password;
    }

    public String getServerUser() {
      return serverUser;
    }

    public String getServerUserPassword() {
      return serverUserPassword;
    }
  }
}
