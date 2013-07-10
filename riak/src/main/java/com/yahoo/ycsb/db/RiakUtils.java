package com.yahoo.ycsb.db;

import static com.google.common.base.Preconditions.*;
import static com.google.common.collect.Maps.newHashMap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.basho.riak.client.IRiakObject;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;

final class RiakUtils {

    private RiakUtils() {
        super();
    }

    static byte[] toBytes(final int anInteger) {

        byte[] aResult = new byte[4];

        aResult[0] = (byte) (anInteger >> 24);
        aResult[1] = (byte) (anInteger >> 16);
        aResult[2] = (byte) (anInteger >> 8);
        aResult[3] = (byte) (anInteger /* >> 0 */);

        return aResult;

    }

    static int fromBytes(final byte[] aByteArray) {

        checkArgument(aByteArray.length == 4);

        return aByteArray[0] << 24 | (aByteArray[1] & 0xFF) << 16
                | (aByteArray[2] & 0xFF) << 8 | (aByteArray[3] & 0xFF);

    }

    static void deserializeTable(final IRiakObject aRiakObject,
            final HashMap<String, ByteIterator> theResult) {

        deserializeTable(aRiakObject.getValue(), theResult);

    }

    static void deserializeTable(final byte[] aValue,
            final Map<String, ByteIterator> theResult) {

        final ByteArrayInputStream anInputStream = new ByteArrayInputStream(
                aValue);

        try {

            byte[] aSizeBuffer = new byte[4];
            while (anInputStream.available() > 0) {

                anInputStream.read(aSizeBuffer);
                final int aColumnNameLength = fromBytes(aSizeBuffer);

                final byte[] aColumnNameBuffer = new byte[aColumnNameLength];
                anInputStream.read(aColumnNameBuffer);

                anInputStream.read(aSizeBuffer);
                final int aColumnValueLength = fromBytes(aSizeBuffer);

                final byte[] aColumnValue = new byte[aColumnValueLength];
                anInputStream.read(aColumnValue);

                theResult.put(new String(aColumnNameBuffer),
                        new ByteArrayByteIterator(aColumnValue));

            }

        } catch (Exception e) {

            throw new IllegalStateException(e);

        } finally {

            close(anInputStream);

        }

    }

    static void close(final InputStream anInputStream) {

        try {
            anInputStream.close();
        } catch (IOException e) {
            // Ignore exception ...
        }

    }

    static void close(final OutputStream anOutputStream) {

        try {
            anOutputStream.close();
        } catch (IOException e) {
            // Ignore exception ...
        }

    }

    static byte[] serializeTable(Map<String, ByteIterator> aTable) {

        final ByteArrayOutputStream anOutputStream = new ByteArrayOutputStream();

        try {

            final Set<Map.Entry<String, ByteIterator>> theEntries = aTable
                    .entrySet();
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

    static <K, V> Map<K, V> merge(final Map<K, V> aMap,
            final Map<K, V> theUpdatedMap) {

        checkNotNull(aMap);
        checkNotNull(theUpdatedMap);

        final Map<K, V> theResult = newHashMap(aMap);

        for (Map.Entry<K, V> aColumn : theUpdatedMap.entrySet()) {
            theResult.put(aColumn.getKey(), aColumn.getValue());
        }

        return theResult;

    }

}
