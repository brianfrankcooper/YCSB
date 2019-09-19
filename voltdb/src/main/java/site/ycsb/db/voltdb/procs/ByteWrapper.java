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

/**
 * Utility class to map data structures used by YCSB to a VoltDB VARBINARY column.
 */
class ByteWrapper {
  private byte[] marr;
  private int moff;
  private int mlen;

  ByteWrapper(byte[] arr, int off, int len) {
    marr = arr;
    moff = off;
    mlen = len;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof ByteWrapper)) {
      return false;
    }
    ByteWrapper that = (ByteWrapper) obj;
    if (this.mlen != that.mlen) {
      return false;
    }
    for (int i = 0; i < this.mlen; i++) {
      if (this.marr[this.moff + i] != that.marr[that.moff + i]) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    if (this.marr == null) {
      return 0;
    }

    int res = 1;
    for (int i = 0; i < mlen; i++) {
      res = 31 * res + marr[moff + i];
    }
    return res;
  }
}