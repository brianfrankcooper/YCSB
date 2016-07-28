/**
 * Copyright (c) 2016 YCSB contributors. All rights reserved.
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

/**
 * BangDB client binding for YCSB.
 *
 */

package com.yahoo.ycsb.db;

import bangdb.*;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import java.util.HashMap;
import java.util.Set;
import java.util.Vector;



/**
 * YCSB binding for <a href="http://bangdb.com/">BangDB</a>.
 *
 * See {@code bangdb/README.md} for details.
 */
public class BangdbClient extends DB {

  private Connection conn;
  private Database db;
  private Table tbl;
  private TableEnv te;
  private static boolean flag = true;
  private static int count = 0;

  // table options for this test
  private static final String DATABASE_NAME = "ycsb";
  private static final String TABLE_NAME = "usertable";
  private static final PersistType PERSIST_TYPE = PersistType.INMEM_PERSIST;
  private static final IndexType INDEX_TYPE = IndexType.BTREE;
  private static final BangDBKeyType KEY_TYPE = BangDBKeyType.NORMAL_KEY;
  private static final Boolean LOG_STATE = false;

  public void init() throws DBException {
    System.loadLibrary("bangdb-client-java");
    BangDBEnv dbenv = BangDBEnv.getInstance();
    db = dbenv.openDatabase(DATABASE_NAME);

    te = new TableEnv();
    te.setPersistType(PERSIST_TYPE);
    te.setIndexType(INDEX_TYPE);
    te.setKeyType(KEY_TYPE);
    te.setLogState(LOG_STATE);

    tbl = db.getTable(TABLE_NAME, DBAccess.OPENCREATE, te);
    conn = tbl.getConnection();

  }

  public void cleanup() throws DBException {
    conn.closeConnection();
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {
    String rs = conn.get(key);

    if (rs == null) {
      System.out.println("READ ERROR");
      return Status.ERROR;
    } else {
      if (fields == null) {
        String[] pairs = rs.split(", field");
        for (int i=0; i<pairs.length; i++) {
          String pair = pairs[i];
          String[] keyValue = pair.split("=", 2);
          result.put("field" + keyValue[0], new StringByteIterator(keyValue[1]));
        }
      } else {
        System.out.println("Scan not implemented");
        return Status.ERROR;
      }
    }
    return result.isEmpty() ? Status.ERROR : Status.OK;
  }

  @Override
  public Status insert(String table, String key,
      HashMap<String, ByteIterator> values) {
    String input = (StringByteIterator.getStringMap(values)).toString();
    return conn.put(key, input, InsertOptions.INSERT_UNIQUE) < 0? Status.ERROR : Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    return conn.del(key) < 0 ? Status.ERROR : Status.OK;
  }

  @Override
  public Status update(String table, String key,
      HashMap<String, ByteIterator> values) {
    String input = (StringByteIterator.getStringMap(values)).toString();
    return conn.put(key, input, InsertOptions.UPDATE_EXISTING) < 0 ? Status.ERROR : Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    ScanFilter sf = new ScanFilter();
    sf.limitBy = ScanLimitBy.LIMIT_RESULT_ROW;
    sf.limit = recordcount;
    sf.setFilter();
    ResultSet rs = null;
    
    rs = conn.scan(startkey, null, sf);
    if (rs != null) {
      HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>(rs.count());
      while(rs.hasNext()) {
        values.put(rs.getNextKeyStr(), new StringByteIterator(rs.getNextValStr()));
        result.add(values);
        rs.moveNext();
        values.clear();
      }
      rs.clear();
      return Status.OK;
    } else {
      return Status.ERROR;
    }
  }

}
