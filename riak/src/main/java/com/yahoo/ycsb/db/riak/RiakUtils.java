/**
 * Copyright (c) 2016 YCSB contributors All rights reserved.
 * Copyright 2014 Basho Technologies, Inc.
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

package com.yahoo.ycsb.db.riak;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.basho.riak.client.api.commands.kv.FetchValue;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Utility class for Riak KV Client.
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

  private static int fromBytes(final byte[] aByteArray) {
    checkArgument(aByteArray.length == 4);

    return (aByteArray[0] << 24) | (aByteArray[1] & 0xFF) << 16 | (aByteArray[2] & 0xFF) << 8 | (aByteArray[3] & 0xFF);
  }

  private static void close(final OutputStream anOutputStream) {
    try {
      anOutputStream.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void close(final InputStream anInputStream) {
    try {
      anInputStream.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Serializes a Map, transforming the contained list of (String, ByteIterator) couples into a byte array.
   *
   * @param aTable A Map to serialize.
   * @return A byte array containng the serialized table.
     */
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

  /**
   * Deserializes an input byte array, transforming it into a list of (String, ByteIterator) pairs (i.e. a Map).
   *
   * @param aValue    A byte array containing the table to deserialize.
   * @param theResult A Map containing the deserialized table.
     */
  private static void deserializeTable(final byte[] aValue, final Map<String, ByteIterator> theResult) {
    final ByteArrayInputStream anInputStream = new ByteArrayInputStream(aValue);
    byte[] aSizeBuffer = new byte[4];

    try {
      while (anInputStream.available() > 0) {
        anInputStream.read(aSizeBuffer);
        final int aColumnNameLength = fromBytes(aSizeBuffer);

        final byte[] aColumnNameBuffer = new byte[aColumnNameLength];
        anInputStream.read(aColumnNameBuffer);

        anInputStream.read(aSizeBuffer);
        final int aColumnValueLength = fromBytes(aSizeBuffer);

        final byte[] aColumnValue = new byte[aColumnValueLength];
        anInputStream.read(aColumnValue);

        theResult.put(new String(aColumnNameBuffer), new ByteArrayByteIterator(aColumnValue));
      }
    } catch (Exception e) {
      throw new IllegalStateException(e);
    } finally {
      close(anInputStream);
    }
  }

  /**
   * Obtains a Long number from a key string. This will be the key used by Riak for all the transactions.
   *
   * @param key The key to convert from String to Long.
   * @return A Long number parsed from the key String.
     */
  static Long getKeyAsLong(String key) {
    String keyString = key.replaceFirst("[a-zA-Z]*", "");

    return Long.parseLong(keyString);
  }

  /**
   * Function that retrieves all the fields searched within a read or scan operation and puts them in the result
   * HashMap.
   *
   * @param fields        The list of fields to read, or null for all of them.
   * @param response      A Vector of HashMaps, where each HashMap is a set field/value pairs for one record.
   * @param resultHashMap The HashMap to return as result.
   */
  static void createResultHashMap(Set<String> fields, FetchValue.Response response,
                                  HashMap<String, ByteIterator>resultHashMap) {
    // If everything went fine, then a result must be given. Such an object is a hash table containing the (field,
    // value) pairs based on the requested fields. Note that in a read operation, ONLY ONE OBJECT IS RETRIEVED!
    // The following line retrieves the previously serialized table which was store with an insert transaction.
    byte[] responseFieldsAndValues = response.getValues().get(0).getValue().getValue();

    // Deserialize the stored response table.
    HashMap<String, ByteIterator> deserializedTable = new HashMap<>();
    deserializeTable(responseFieldsAndValues, deserializedTable);

    // If only specific fields are requested, then only these should be put in the result object!
    if (fields != null) {
      // Populate the HashMap to provide as result.
      for (Object field : fields.toArray()) {
        // Comparison between a requested field and the ones retrieved. If they're equal (i.e. the get() operation
        // DOES NOT return a null value), then  proceed to store the pair in the resultHashMap.
        ByteIterator value = deserializedTable.get(field);

        if (value != null) {
          resultHashMap.put((String) field, value);
        }
      }
    } else {
      // If, instead, no field is specified, then all those retrieved must be provided as result.
      for (String field : deserializedTable.keySet()) {
        resultHashMap.put(field, deserializedTable.get(field));
      }
    }
  }
}
