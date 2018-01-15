package com.yahoo.ycsb.db;

import com.yahoo.ycsb.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * Test the Akumuli binding with some basic checks.
 *
 * @author Rene Trefft
 */
public final class AkumuliTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(AkumuliTest.class);

  private AkumuliClient client;

  @Before
  public void setup() throws DBException {
    client = new AkumuliClient();
    client.init();
  }

  @After
  public void teardown() throws DBException {
    client.cleanup();
  }

  @Test
  @Ignore // requires local installation of akumuli.
  public void tests() {
    HashMap<String, ByteIterator> tagsInsert = new HashMap<>();
    tagsInsert.put("tag1_Key", new StringByteIterator("tag1_Val1"));
    HashMap<String, ByteIterator> tagsInsert2 = new HashMap<>();
    tagsInsert2.put("tag1_Key", new StringByteIterator("tag1_Val2"));

    LOGGER.info("Starting insertion checks");
    // Insert tests
    assertEquals(Status.OK, client.insert("metric1", Timestamp.valueOf("2001-10-19 20:15:00").getTime(), 1.0, tagsInsert));
    assertEquals(Status.OK, client.insert("metric1", Timestamp.valueOf("2001-10-19 20:15:00").getTime(), 1.5, tagsInsert2));
    assertEquals(Status.OK, client.insert("metric1", Timestamp.valueOf("2013-01-20 00:00:00").getTime(), 2.0, tagsInsert));
    assertEquals(Status.OK, client.insert("metric2", Timestamp.valueOf("2016-04-03 19:56:00").getTime(), 3.0, tagsInsert));

    assertEquals(Status.OK, client.insert("metric2", Timestamp.valueOf("2016-04-04 09:30:00").getTime(), 1.5, tagsInsert2));
    assertEquals(Status.OK, client.insert("metric2", Timestamp.valueOf("2016-04-04 09:40:00").getTime(), 2.5, tagsInsert2));
    assertEquals(Status.OK, client.insert("metric2", Timestamp.valueOf("2016-04-04 09:50:00").getTime(), 3.5, tagsInsert2));
    assertEquals(Status.OK, client.insert("metric2", Timestamp.valueOf("2016-04-04 09:55:00").getTime(), 4.5, tagsInsert2));
    LOGGER.info("Insert-Checks successful - Starting Query checks");

    HashMap<String, List<String>> tagsQuery = new HashMap<>();
    List<String> tagValues = new ArrayList<>();
    tagValues.add("tag1_Val1");
    tagValues.add("tag1_Val2");
    tagsQuery.put("tag1_Key", tagValues);

    // Read tests
    assertEquals(Status.OK, client.read("metric1", Timestamp.valueOf("2001-10-19 20:15:00").getTime(), tagsQuery));
    assertEquals(Status.OK, client.read("metric2", Timestamp.valueOf("2016-04-03 19:56:00").getTime(), tagsQuery));
    LOGGER.info("Query-Checks successful - Starting scan-check");

    // Scan tests
    assertEquals(Status.OK, client.scan("metric2", Timestamp.valueOf("2016-04-04 09:30:00").getTime(),
        Timestamp.valueOf("2016-04-04 10:00:00").getTime(), tagsQuery,
        TimeseriesDB.AggregationOperation.AVERAGE, 15, TimeUnit.MINUTES));
    LOGGER.info("All Checks successful");
  }
}
