/*
 * Copyright 2017 YCSB contributors. All Rights Reserved.
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

import com.google.cloud.spanner.*;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Google Cloud Spanner Client for YCSB.
 *
 * To use it, manually create an instance and a database with a table (e.g. in the cloud console). The
 * table and database can have any name, but columns must be "pkey" (string, primary key), and
 * "field0", "field1", .... "fieldN" depending on the number of fields in the workload.
 *
 * At minimum specify these properties:
 *
 * <ul>
 *     <li> googlespanner.instance - name of your spanner instance</li>
 *     <li> googlespanner.database - name of your spanner database</li>
 *     <li> table - name of the table you created in spanner</li>
 * </ul>
 *
 * This binding assumes application default credentials and cannot currently be used with any other authentication
 * mechanism.
 */

public class GoogleSpannerClient extends DB {
  private static Logger logger =
      Logger.getLogger(GoogleSpannerClient.class);

  private Spanner spanner;
  private DatabaseClient dbClient;

  @Override
  public void init() throws DBException {
    String debug = getProperties().getProperty("googlespanner.debug", null);
    if (null != debug && "true".equalsIgnoreCase(debug)) {
      logger.setLevel(Level.DEBUG);
    }

    final String instanceId = getProperties().getProperty(
        "googlespanner.instance", null);
    if (instanceId == null) {
      throw new DBException(
          "Required property \"googlespanner.instance\" missing.");
    }
    final String database = getProperties().getProperty(
        "googlespanner.database", null);
    if (database == null) {
      throw new DBException(
          "Required property \"googlespanner.database\" missing.");
    }

    final SpannerOptions options = SpannerOptions.newBuilder().build();
    this.spanner = options.getService();
    final DatabaseId db = DatabaseId.of(options.getProjectId(), instanceId, database);
    this.dbClient = spanner.getDatabaseClient(db);
  }

  @Override
  public void cleanup() throws DBException {
    super.cleanup();

    try {
      this.spanner.closeAsync().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new DBException(e);
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
          HashMap<String, ByteIterator> result) {
    // XXX: Key is not escaped. Unclear to me at this time how to do this in Spanner's SQL dialect.
    final ResultSet resultSet =
        dbClient
            .singleUse()
            .executeQuery(
                Statement.of(
                    "SELECT " + columnsToSelectSql(fields) + "\n"
                        + "FROM `" + table + "`\n"
                        + "WHERE pkey = '" + key + "'"));
    if (resultSet.next()) {
      if (fields != null) {
        for (String field : fields) {
          final String value = resultSet.getString(field);
          result.put(field, new StringByteIterator(value));
        }
      }
    } else {
      return Status.NOT_FOUND;
    }

    if (resultSet.next()) {
      return Status.UNEXPECTED_STATE;
    }

    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(String table, String key,
      HashMap<String, ByteIterator> values) {
    return insert(table, key, values);
  }

  @Override
  public Status insert(String table, String key,
      HashMap<String, ByteIterator> values) {
    final Mutation.WriteBuilder wb = Mutation.newInsertOrUpdateBuilder(table)
        .set("pkey")
        .to(key);

    for (Map.Entry<String, ByteIterator> kv : values.entrySet()) {
      wb.set(kv.getKey()).to(kv.getValue().toString());
    }

    final Mutation mutation = wb.build();

    this.dbClient.write(Collections.singletonList(mutation));

    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    final Key spannerKey = Key.of(key);

    this.dbClient.write(Collections.singletonList(Mutation.delete(table, spannerKey)));

    return Status.OK;
  }

  private String columnsToSelectSql(Set<String> fields) {
    if (fields == null) {
      return "*";
    }

    boolean first = true;
    final StringBuilder sb = new StringBuilder();
    for (String field : fields) {
      if (first) {
        first = false;
      } else {
        sb.append(",");
      }
      sb.append("`");
      sb.append(field);
      sb.append("`");
    }

    return sb.toString();
  }
}
