/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
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

package com.yahoo.ycsb;

import java.util.Random;

/**
 * Utility functions.
 */
public final class Utils {
  public static final int FNV_OFFSET_BASIS_32 = 0x811c9dc5;
  public static final long FNV_OFFSET_BASIS_64 = 0xCBF29CE484222325L;

  public static final int FNV_PRIME_32 = 16777619;
  public static final long FNV_PRIME_64 = 1099511628211L;

  private static final Random RANDOM;

  private static final ThreadLocal<Random> LOCAL_RANDOM;

  static {
    RANDOM = new Random();
    LOCAL_RANDOM = new ThreadLocal<Random>();
  }
  
  /**
   * Generate a random ASCII string of a given length.
   */
  public static String asciiString(final int length) {
    final int interval = ('~' - ' ') + 1;

    final byte[] buf = new byte[length];
    random().nextBytes(buf);
    for (int i = 0; i < length; i++) {
      if (buf[i] < 0) {
        buf[i] = (byte) ((-buf[i] % interval) + ' ');
      } else {
        buf[i] = (byte) ((buf[i] % interval) + ' ');
      }
    }
    return new String(buf);
  }

  /**
   * 32 bit FNV hash. Produces more "random" hashes than (say)
   * String.hashCode().
   * 
   * @param val
   *          The value to hash.
   * @return The hash value
   */
  public static int fnvhash32(int val) {
    // from http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
    int hashval = FNV_OFFSET_BASIS_32;

    for (int i = 0; i < 4; i++) {
      final int octet = val & 0x00ff;
      val = val >> 8;

      hashval = hashval ^ octet;
      hashval = hashval * FNV_PRIME_32;
      // hashval = hashval ^ octet;
    }
    return Math.abs(hashval);
  }

  /**
   * 64 bit FNV hash. Produces more "random" hashes than (say)
   * String.hashCode().
   * 
   * @param val
   *          The value to hash.
   * @return The hash value
   */
  public static long fnvHash64(long val) {
    // from http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
    long hashval = FNV_OFFSET_BASIS_64;

    for (int i = 0; i < 8; i++) {
      final long octet = val & 0x00ff;
      val = val >> 8;

      hashval = hashval ^ octet;
      hashval = hashval * FNV_PRIME_64;
      // hashval = hashval ^ octet;
    }
    return Math.abs(hashval);
  }

  /**
   * Hash a long value.
   */
  public static long hash(final long val) {
    return fnvHash64(val);
  }

  public static Random random() {
    Random ret = LOCAL_RANDOM.get();
    if (ret == null) {
      ret = new Random(RANDOM.nextLong());
      LOCAL_RANDOM.set(ret);
    }
    return ret;
  }

  /**
   * Hidden Constructor.
   */
  private Utils() {
    // Nothing.
  }
}
