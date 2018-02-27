/*
 * Copyright (c) 2015 - 2018 Andreas Bader, Rene Trefft 2018 YCSB Contributors All rights reserved.
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
package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.TimeseriesDB;
import kx.c;
import kx.c.KException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * kdb+ client for YCSB framework. Insertions will be stored on memory, so after
 * shutdown data is lost.
 *
 * @author Rene Trefft
 */
public class KdbPlusClient extends TimeseriesDB {

  private static final String PORT_PROPERTY = "port";
  private static final String PORT_PROPERTY_DEFAULT = "5001";

  private static final String IP_PROPERTY = "ip";
  private static final String IP_PROPERTY_DEFAULT = "localhost";

  private c kdbPlus;

  @Override
  public void init() throws DBException {
    super.init();
    final Properties properties = getProperties();
    if (debug) {
      LOGGER.info("The following properties are given: ");
      for (String element : properties.stringPropertyNames()) {
        LOGGER.info("{}: {}", element, properties.getProperty(element));
      }
    }

    if (!test) {
      if (!properties.containsKey(PORT_PROPERTY)) {
        throwMissingProperty(PORT_PROPERTY);
      }
      if (!properties.containsKey(IP_PROPERTY)) {
        throwMissingProperty(IP_PROPERTY);
      }
      int port = Integer.parseInt(properties.getProperty(PORT_PROPERTY, PORT_PROPERTY_DEFAULT));
      String ip = properties.getProperty(IP_PROPERTY, IP_PROPERTY_DEFAULT);
      try {
        // establishes connection to kdb+
        this.kdbPlus = new c(ip, port);
      } catch (KException | IOException e) {
        throw new DBException(e);
      }
    }
  }

  @Override
  public void cleanup() throws DBException {
    try {
      kdbPlus.close();
    } catch (IOException e) {
      throw new DBException(e);
    }
  }

  @Override
  public Status read(String metric, Long timestamp, Map<String, List<String>> tags) {
    if (metric == null || metric.equals("") || timestamp == null) {
      return Status.BAD_REQUEST;
    }

    StringBuilder query = new StringBuilder("select from ");
    query.append(metric);
    query.append(" where(timestamp=\"P\"$\"");
    query.append(timestamp.toString());
    query.append("\")");

    generateTagFilterQueryPart(query, tags);

    if (debug) {
      LOGGER.info("Query: {}", query);
    }
    if (test) {
      return Status.OK;
    }
    return execQuery(query.toString());
  }

  @Override
  public Status scan(String metric, Long startTs, Long endTs, Map<String, List<String>> tags,
                     AggregationOperation aggreg, int timeValue, TimeUnit timeUnit) {

    if (metric == null || metric.isEmpty() || startTs == null || endTs == null) {
      return Status.BAD_REQUEST;
    }

    StringBuilder query = new StringBuilder("select ");
    if (aggreg == AggregationOperation.AVERAGE) {
      query.append("avg ");
    } else if (aggreg == AggregationOperation.COUNT) {
      query.append("count ");
    } else if (aggreg == AggregationOperation.SUM) {
      query.append("sum ");
    }

    // Resolution / down sampling is not supported by kdb+ out-of-the-box.
    // We use the built-in xbar function which returns in the given time
    // range for example the average for every 1 second.

    query.append("val by(");
    query.append(timeValue);

    if (timeUnit != TimeUnit.NANOSECONDS) {
      query.append("*");
      if (timeUnit == TimeUnit.MICROSECONDS) {
        query.append("1000");
      } else if (timeUnit == TimeUnit.MILLISECONDS) {
        query.append("1000000");
      } else if (timeUnit == TimeUnit.SECONDS) {
        query.append("1000000000");
      } else if (timeUnit == TimeUnit.MINUTES) {
        query.append("60000000000");
      } else if (timeUnit == TimeUnit.HOURS) {
        query.append("3600000000000");
      } else if (timeUnit == TimeUnit.DAYS) {
        query.append("86400000000000");
      } else {
        return Status.BAD_REQUEST;
      }
    }

    query.append(")xbar timestamp");

    query.append(" from ");
    query.append(metric);

    query.append(" where(timestamp>=\"P\"$\"");
    query.append(startTs.toString());
    query.append("\")");
    query.append(",(timestamp<=\"P\"$\"");
    query.append(endTs.toString());
    query.append("\")");

    generateTagFilterQueryPart(query, tags);
    if (debug) {
      LOGGER.info("Query: {}", query);
    }
    if (test) {
      return Status.OK;
    }
    return execQuery(query.toString());
  }

  /**
   * Sends {@code query} to the kdb+ instance which executes it.
   */
  private Status execQuery(String query) {
    try {
      kdbPlus.k(query);
      return Status.OK;
    } catch (KException | IOException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  /**
   * Generates the tag filter query part. The query part will be appended to
   * StringBuilder {@code query}.
   *
   * @param tags to use for filtering
   */
  private void generateTagFilterQueryPart(StringBuilder query, Map<String, List<String>> tags) {
    for (Map.Entry<String, List<String>> tag : tags.entrySet()) {
      query.append(',');
      for (String tagValue : tag.getValue()) {
        query.append("(tags[;`$\"");
        // We assume that the tag keys used in the queries are defined
        // in all inserted rows, more precisely in their tags
        // dictionary. Otherwise a type error is thrown and we have to
        // use the match operator.
        // See: http://stackoverflow.com/questions/33989718/select-query-on-kdb-tsdb-table-with-nested-dictionary
        query.append(tag.getKey());
        query.append("\"]=`$\"");
        query.append(tagValue);
        query.append("\")or");
      }
      // deletes the "or" after the last value of the actual tag
      deleteLastChars(query, 2);
    }
  }

  /**
   * Generates a query part which creates a table with name {@code table} if
   * there is not already a table with that name. The query part will be
   * appended to StringBuilder {@code query}.
   *
   * @param table name - must only consists of letters, numbers and underscores
   *              (case-sensitive), no whitespaces are allowed
   */
  private void generateCreateTableIfNotExistQueryPart(StringBuilder query, String table) {
    query.append("if[not`");
    query.append(table);
    query.append(" in tables[];");
    query.append(table);
    query.append(":([]timestamp:-12h$();val:-9h$();tags:())];");
  }

  /**
   * @param sb       to delete chars from
   * @param numChars to delete at the end
   */
  private void deleteLastChars(StringBuilder sb, int numChars) {
    sb.delete(sb.length() - numChars, sb.length());
  }

  /**
   * {@inheritDoc}
   * <p>
   * {@code metric} must only consists of letters, numbers and underscores
   * (case-sensitive), no whitespaces are allowed.
   */
  @Override
  public Status insert(String metric, Long timestamp, double value, Map<String, ByteIterator> tags) {
    if (metric == null || metric.isEmpty() || timestamp == null) {
      return Status.BAD_REQUEST;
    }

    StringBuilder query = new StringBuilder();
    generateCreateTableIfNotExistQueryPart(query, metric);
    query.append('`');
    query.append(metric);
    query.append(" insert(\"P\"$\"");
    query.append(timestamp.toString());
    query.append("\";");
    query.append(value);
    query.append(';');

    int numTags = tags.size();

    if (numTags > 1) {

      StringBuilder tagKeysQueryPart = new StringBuilder("(`$(");
      StringBuilder tagValuesQueryPart = new StringBuilder("`$(");
      for (Map.Entry<String, ByteIterator> tag : tags.entrySet()) {
        tagKeysQueryPart.append('"');
        tagKeysQueryPart.append(tag.getKey());
        tagKeysQueryPart.append("\";");
        tagValuesQueryPart.append('"');
        tagValuesQueryPart.append(tag.getValue());
        tagValuesQueryPart.append("\";");
      }
      // deletes the last ";"
      deleteLastChars(tagKeysQueryPart, 1);
      deleteLastChars(tagValuesQueryPart, 1);

      tagKeysQueryPart.append("))");
      tagValuesQueryPart.append("))");

      query.append(tagKeysQueryPart);
      query.append('!');
      query.append(tagValuesQueryPart);
    } else if (numTags == 1) {
      // prefix 'enlist' is necessary for dict with one key/value
      query.append("(enlist`$\"");
      Entry<String, ByteIterator> tag = tags.entrySet().iterator().next();
      query.append(tag.getKey());
      query.append("\")!enlist`$\"");
      query.append(tag.getValue());
      query.append("\")");
    } else {
      // char '-' as tags => no tags are defined
      query.append("-)");
    }

    if (debug) {
      LOGGER.info("Query: {}", query);
    }
    if (test) {
      return Status.OK;
    }
    return execQuery(query.toString());
  }

  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }
}
