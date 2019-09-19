/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors All rights reserved.
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

package site.ycsb;

import java.util.HashMap;
import java.util.Map;

/**
 * A ByteIterator that iterates through a string.
 */
public class StringByteIterator extends ByteIterator {
  private String str;
  private int off;

  /**
   * Put all of the entries of one map into the other, converting
   * String values into ByteIterators.
   */
  public static void putAllAsByteIterators(Map<String, ByteIterator> out, Map<String, String> in) {
    for (Map.Entry<String, String> entry : in.entrySet()) {
      out.put(entry.getKey(), new StringByteIterator(entry.getValue()));
    }
  }

  /**
   * Put all of the entries of one map into the other, converting
   * ByteIterator values into Strings.
   */
  public static void putAllAsStrings(Map<String, String> out, Map<String, ByteIterator> in) {
    for (Map.Entry<String, ByteIterator> entry : in.entrySet()) {
      out.put(entry.getKey(), entry.getValue().toString());
    }
  }

  /**
   * Create a copy of a map, converting the values from Strings to
   * StringByteIterators.
   */
  public static Map<String, ByteIterator> getByteIteratorMap(Map<String, String> m) {
    HashMap<String, ByteIterator> ret =
        new HashMap<String, ByteIterator>();

    for (Map.Entry<String, String> entry : m.entrySet()) {
      ret.put(entry.getKey(), new StringByteIterator(entry.getValue()));
    }
    return ret;
  }

  /**
   * Create a copy of a map, converting the values from
   * StringByteIterators to Strings.
   */
  public static Map<String, String> getStringMap(Map<String, ByteIterator> m) {
    HashMap<String, String> ret = new HashMap<String, String>();

    for (Map.Entry<String, ByteIterator> entry : m.entrySet()) {
      ret.put(entry.getKey(), entry.getValue().toString());
    }
    return ret;
  }

  public StringByteIterator(String s) {
    this.str = s;
    this.off = 0;
  }

  @Override
  public boolean hasNext() {
    return off < str.length();
  }

  @Override
  public byte nextByte() {
    byte ret = (byte) str.charAt(off);
    off++;
    return ret;
  }

  @Override
  public long bytesLeft() {
    return str.length() - off;
  }

  @Override
  public void reset() {
    off = 0;
  }

  @Override
  public byte[] toArray() {
    byte[] bytes = new byte[(int) bytesLeft()];
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte) str.charAt(off + i);
    }
    off = str.length();
    return bytes;
  }

  /**
   * Specialization of general purpose toString() to avoid unnecessary
   * copies.
   * <p>
   * Creating a new StringByteIterator, then calling toString()
   * yields the original String object, and does not perform any copies
   * or String conversion operations.
   * </p>
   */
  @Override
  public String toString() {
    if (off > 0) {
      return super.toString();
    } else {
      return str;
    }
  }
}
