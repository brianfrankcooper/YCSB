package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.TimeseriesDB;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Manual {

  public static void main(String[] args) throws DBException {
    DatabusClient client = new DatabusClient();
    client.init();

    Map<String, ByteIterator> tagsInsert = new HashMap<>();
    tagsInsert.put("TAG0", new StringByteIterator("tag0_Val"));
    tagsInsert.put("TAG1", new StringByteIterator("tag1_Val"));
    tagsInsert.put("TAG2", new StringByteIterator("tag2_Val"));

    // Insert tests
    System.out.println(client.insert("usermetric", Timestamp.valueOf("2001-10-19 20:15:00").getTime(), 1.0, tagsInsert));
    System.out.println(client.insert("usermetric", Timestamp.valueOf("2016-02-11 10:59:50").getTime(), 2.0, tagsInsert));
    System.out.println(client.insert("usermetric", Timestamp.valueOf("2016-02-11 11:00:00").getTime(), 3.0, tagsInsert));
    System.out.println(client.insert("usermetric", Timestamp.valueOf("2016-02-11 11:15:00").getTime(), 4.0, tagsInsert));
    System.out.println(client.insert("usermetric", Timestamp.valueOf("2016-02-11 11:16:00").getTime(), 5.0, tagsInsert));

    Map<String, List<String>> tagsQuery = new HashMap<>();

    List<String> tag0Values = new ArrayList<>();
    tag0Values.add("tag0_Val");
    tag0Values.add("tag0_Val2");
    tagsQuery.put("TAG0", tag0Values);

    List<String> tag1Values = new ArrayList<>();
    tag1Values.add("tag1_Val");
    tag1Values.add("tag1_Val2");

    tagsQuery.put("TAG1", tag1Values);

    List<String> tag2Values = new ArrayList<>();
    tag2Values.add("tag2_Val");
    tag2Values.add("tag2_Val2");
    tagsQuery.put("TAG2", tag2Values);

    // Read tests
    System.out.println(client.read("usermetric", Timestamp.valueOf("2001-10-19 20:15:00").getTime(), tagsQuery));
    System.out.println(client.read("usermetric", Timestamp.valueOf("2016-02-11 11:00:00").getTime(), tagsQuery));

    // Reading not existing records => should return -1
    System.out.println(client.read("usermetric", Timestamp.valueOf("2013-01-20 00:00:01").getTime(), tagsQuery));

    // Scan tests
    System.out.println(client.scan("usermetric", Timestamp.valueOf("2016-02-11 10:00:00").getTime(),
        Timestamp.valueOf("2016-02-11 11:30:00").getTime(), tagsQuery, TimeseriesDB.AggregationOperation.NONE,
        10, TimeUnit.MINUTES));

  }

}
