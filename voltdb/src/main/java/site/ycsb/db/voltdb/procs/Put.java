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

import java.nio.ByteBuffer;
import java.util.HashSet;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

/**
 * 
 * Update a value in STORE.
 * 
 */
public class Put extends VoltProcedure {
  private final SQLStmt selectStmt = new SQLStmt("SELECT value FROM Store WHERE keyspace = ? AND key = ?");
  private final SQLStmt insertStmt = new SQLStmt("INSERT INTO Store VALUES (?, ?, ?)");
  private final SQLStmt updateStmt = new SQLStmt("UPDATE Store SET value = ? WHERE keyspace = ? AND key = ?");

  public long run(byte[] keyspace, String key, byte[] data) {
    voltQueueSQL(selectStmt, keyspace, key);
    VoltTable res = voltExecuteSQL()[0];
    if (res.advanceRow()) {
      voltQueueSQL(updateStmt, merge(res.getVarbinary(0), data), keyspace, key);
    } else {
      voltQueueSQL(insertStmt, keyspace, key, data);
    }
    voltExecuteSQL(true);
    return 0L;
  }

  private byte[] merge(byte[] dest, byte[] src) {
    HashSet<ByteWrapper> mergeSet = new HashSet<ByteWrapper>();
    ByteBuffer buf = ByteBuffer.wrap(src);
    int nSrc = buf.getInt();
    for (int i = 0; i < nSrc; i++) {
      int len = buf.getInt();
      int off = buf.position();
      mergeSet.add(new ByteWrapper(src, off, len));
      buf.position(off + len);
      len = buf.getInt();
      buf.position(buf.position() + len);
    }

    byte[] merged = new byte[src.length + dest.length];
    ByteBuffer out = ByteBuffer.wrap(merged);

    buf = ByteBuffer.wrap(dest);
    int nDest = buf.getInt();
    int nFields = nSrc + nDest;
    out.putInt(nFields);

    int blockStart = 4;
    int blockEnd = 4;
    for (int i = 0; i < nDest; i++) {
      int len = buf.getInt();
      int off = buf.position();
      boolean flushBlock = mergeSet.contains(new ByteWrapper(dest, off, len));
      buf.position(off + len);
      len = buf.getInt();
      buf.position(buf.position() + len);
      if (flushBlock) {
        if (blockStart < blockEnd) {
          out.put(dest, blockStart, blockEnd - blockStart);
        }
        nFields--;
        blockStart = buf.position();
      }
      blockEnd = buf.position();
    }
    if (blockStart < blockEnd) {
      out.put(dest, blockStart, blockEnd - blockStart);
    }
    out.put(src, 4, src.length - 4);

    int length = out.position();
    if (nFields != nSrc + nDest) {
      out.position(0);
      out.putInt(nFields);
    }

    byte[] res = new byte[length];
    System.arraycopy(merged, 0, res, 0, length);
    return res;
  }
}
