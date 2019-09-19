/**
 * Copyright (c) 2015-2019 YCSB contributors. All rights reserved.
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

package site.ycsb.db.voltdb.procs;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

/**
 * 
 * Query STORE using a single partition query.
 * 
 */
public class Scan extends VoltProcedure {

  private final SQLStmt getBddStmt = new SQLStmt(
      "SELECT value, key FROM Store WHERE keyspace = ? AND key >= ? ORDER BY key, keyspace LIMIT ?");
  private final SQLStmt getUnbddStmt = new SQLStmt(
      "SELECT value, key FROM Store WHERE keyspace = ? ORDER BY key, keyspace LIMIT ?");

  public VoltTable[] run(String partKey, byte[] keyspace, byte[] rangeMin, int count) throws Exception {
    if (rangeMin != null) {
      voltQueueSQL(getBddStmt, keyspace, new String(rangeMin, "UTF-8"), count);
    } else {
      voltQueueSQL(getUnbddStmt, keyspace, count);
    }
    return voltExecuteSQL(true);
  }
}
