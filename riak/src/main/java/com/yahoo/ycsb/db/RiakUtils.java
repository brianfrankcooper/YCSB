/*
 * Copyright 2016 nygard_89
 * Copyright 2014 Basho Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.ycsb.db;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Set;

import com.yahoo.ycsb.ByteIterator;

/**
 * @author nygard_89
 * @author Basho Technologies, Inc.
 *
 */
final class RiakUtils {

  private RiakUtils() {
    super();
  }

  private static byte[] toBytes(final int anInteger) {
    byte[] aResult = new byte[4];

    aResult[0] = (byte) (anInteger >> 24);
    aResult[1] = (byte) (anInteger >> 16);
    aResult[2] = (byte) (anInteger >> 8);
    aResult[3] = (byte) (anInteger /* >> 0 */);

    return aResult;
  }

  private static void close(final OutputStream anOutputStream) {
    try {
      anOutputStream.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  static byte[] serializeTable(Map<String, ByteIterator> aTable) {
    final ByteArrayOutputStream anOutputStream = new ByteArrayOutputStream();
    final Set<Map.Entry<String, ByteIterator>> theEntries = aTable.entrySet();

    try {
      for (final Map.Entry<String, ByteIterator> anEntry : theEntries) {
        final byte[] aColumnName = anEntry.getKey().getBytes();

        anOutputStream.write(toBytes(aColumnName.length));
        anOutputStream.write(aColumnName);

        final byte[] aColumnValue = anEntry.getValue().toArray();

        anOutputStream.write(toBytes(aColumnValue.length));
        anOutputStream.write(aColumnValue);
      }
      return anOutputStream.toByteArray();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    } finally {
      close(anOutputStream);
    }
  }

  static Long getKeyAsLong(String key) {
    String keyString = key.replace("user", "").replaceFirst("^0*", "");
    return Long.parseLong(keyString);
  }
}
