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

import java.util.HashMap;
import java.util.Map;

/**
 * {@link ByteIterator} implementation that wraps a {@code String}.
 */
public class StringByteIterator extends ByteIterator {
  /**
   * Create a copy of a map, converting the values from Strings to
   * StringByteIterators.
   */
  public static HashMap<String, ByteIterator> getByteIteratorMap(
      final Map<String, String> m) {
    final HashMap<String, ByteIterator> ret =
        new HashMap<String, ByteIterator>();

    for (final String s : m.keySet()) {
      ret.put(s, new StringByteIterator(m.get(s)));
    }
    return ret;
  }

  /**
   * Create a copy of a map, converting the values from StringByteIterators to
   * Strings.
   */
  public static HashMap<String, String> getStringMap(
      final Map<String, ByteIterator> m) {
    final HashMap<String, String> ret = new HashMap<String, String>();

    for (final String s : m.keySet()) {
      ret.put(s, m.get(s).toString());
    }
    return ret;
  }

  /**
   * Put all of the entries of one map into the other, converting String values
   * into ByteIterators.
   */
  public static void putAllAsByteIterators(final Map<String, ByteIterator> out,
      final Map<String, String> in) {
    for (final String s : in.keySet()) {
      out.put(s, new StringByteIterator(in.get(s)));
    }
  }

  /**
   * Put all of the entries of one map into the other, converting ByteIterator
   * values into Strings.
   */
  public static void putAllAsStrings(final Map<String, String> out,
      final Map<String, ByteIterator> in) {
    for (final String s : in.keySet()) {
      out.put(s, in.get(s).toString());
    }
  }

  private int off;

  private final String str;

  public StringByteIterator(final String s) {
    this.str = s;
    this.off = 0;
  }

  @Override
  public long bytesLeft() {
    return str.length() - off;
  }

  @Override
  public boolean hasNext() {
    return off < str.length();
  }

  @Override
  public byte nextByte() {
    final byte ret = (byte) str.charAt(off);
    off++;
    return ret;
  }

  /**
   * Specialization of general purpose toString() to avoid unnecessary copies.
   * <p>
   * Creating a new StringByteIterator, then calling toString() yields the
   * original String object, and does not perform any copies or String
   * conversion operations.
   * </p>
   */
  @Override
  public String toString() {
    if (off > 0) {
      return super.toString();
    }
    return str;
  }
}
