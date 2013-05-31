package com.yahoo.ycsb.db;

import static com.google.common.collect.Maps.*;
import static org.testng.Assert.*;
import static com.yahoo.ycsb.db.RiakUtils.*;

import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;

public final class RiakUtilsTest {

    private static final String INTEGER_DATA_PROVIDER = "integer_data_provider";
    private static final String TABLE_DATA_PROVIDER = "table_data_provider";

    @Test(dataProvider = INTEGER_DATA_PROVIDER)
    public void testIntegerByteConversion(final int anExpectedValue) {

        final byte[] aByteArray = toBytes(anExpectedValue);
        assertTrue(aByteArray.length == 4);

        final int anActualValue = fromBytes(aByteArray);
        assertEquals(anActualValue, anExpectedValue);

    }

    @Test(dataProvider = TABLE_DATA_PROVIDER)
    public void testTableSerialization(
            final HashMap<String, ByteIterator> anExpectedTable) {

        assertNotNull(anExpectedTable);

        final byte[] aSerializedTable = serializeTable(anExpectedTable);

        assertTrue(aSerializedTable.length > 0);

        final HashMap<String, ByteIterator> anActualTable = newHashMap();
        deserializeTable(aSerializedTable, anActualTable);

        System.out.println("Expected Table " + anExpectedTable);
        System.out.println("Actual Table " + anActualTable);

        for (final Map.Entry<String, ByteIterator> aColumn : anExpectedTable
                .entrySet()) {

            assertEquals(anActualTable.get(aColumn.getKey()).toArray(),
                    anExpectedTable.get(aColumn.getKey()).toArray());

        }

    }

    @DataProvider(name = INTEGER_DATA_PROVIDER)
    public Object[][] provideIntegerData() {

        return new Object[][] { { 1 }, { 0 }, { -1 }, { 1000 }, { -1000 } };

    }

    @DataProvider(name = TABLE_DATA_PROVIDER)
    public Object[][] provideTableData() {

        Object[][] theData = new Object[1][1];

        HashMap<String, ByteIterator> aTable = newHashMap();
        aTable.put("foo", new ByteArrayByteIterator("zoodles".getBytes()));
        theData[0][0] = aTable;

        return theData;

    }
}
