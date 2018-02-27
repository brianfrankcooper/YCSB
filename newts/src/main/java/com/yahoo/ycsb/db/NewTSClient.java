/*
 * Copyright (c) 2015 - 2018 Andreas Bader 2018 YCSB Contributors All rights reserved.
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

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Optional;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.TimeseriesDB;
import org.opennms.newts.api.*;
import org.opennms.newts.api.query.ResultDescriptor;
import org.opennms.newts.api.query.StandardAggregationFunctions;
import org.opennms.newts.api.search.*;
import org.opennms.newts.cassandra.CassandraSession;
import org.opennms.newts.cassandra.CassandraSessionImpl;
import org.opennms.newts.cassandra.ContextConfigurations;
import org.opennms.newts.cassandra.search.CassandraSearcher;
import org.opennms.newts.persistence.cassandra.CassandraSampleRepository;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.yahoo.ycsb.TimeseriesDB.AggregationOperation.COUNT;
import static com.yahoo.ycsb.TimeseriesDB.AggregationOperation.SUM;

/**
 * NewTS client for YCSB framework.
 * No SUM or COUNT available, using MAX/MIN instead
 * Filtering for Tags and Time (together) is not possible,
 * Filtering for Tags and Aggregation is not possible
 * Filtering for Tags does not work properly, no support for SCAN/SUM/AVG/COUNT + Filtering for Tags
 * Buckets/Granularity is not possible to be "on", using 1 year size instead
 */
public class NewTSClient extends TimeseriesDB {

  private static final String IP_PROPERTY = "ip";
  private static final String IP_PROPERTY_DEFAULT = "localhost";

  private static final String PORT_PROPERTY = "port";
  private static final String PORT_PROPERTY_DEFAULT = "9042";

  private static final String KEYSPACE_PROPERTY = "keyspace";
  private static final String KEYSPACE_PROPERTY_DEFAULT = "newts";

  private static final String HEARTBEAT_FACTOR_PROPERTY = "heartbeatFactor";
  private static final String HEARTBEAT_FACTOR_PROPERTY_DEFAULT = "10";

  private static final String FILTER_FOR_TAGS_PROPERTY = "filterForTags";
  private static final String FILTER_FOR_TAGS_PROPERTY_DEFAULT = "false";

  private CassandraSession client;
  private SampleRepository repo;
  private CassandraSearcher searcher;
  private ContextConfigurations contextConfigurations = new ContextConfigurations();
  // NewTS can only filter for tags OR for time, tags + avg/sum/count/scan does not work
  private boolean filterForTags;

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
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
    if (!properties.containsKey(PORT_PROPERTY) && !test) {
      throwMissingProperty(PORT_PROPERTY);
    }
    if (!properties.containsKey(IP_PROPERTY) && !test) {
      throwMissingProperty(IP_PROPERTY);
    }
    filterForTags = Boolean.parseBoolean(properties.getProperty(FILTER_FOR_TAGS_PROPERTY,
        FILTER_FOR_TAGS_PROPERTY_DEFAULT));
    String keySpace = properties.getProperty(KEYSPACE_PROPERTY, KEYSPACE_PROPERTY_DEFAULT);
    int port = Integer.parseInt(properties.getProperty(PORT_PROPERTY, PORT_PROPERTY_DEFAULT));
    String ip = properties.getProperty(IP_PROPERTY, IP_PROPERTY_DEFAULT);
    try {
      if (!test) {
        SampleProcessorService processors = new DefaultSampleProcessorService(32);
        final int ttl = 86400 * 365; // 1 Jahr TTL
        MetricRegistry registry = new MetricRegistry();
        client = new CassandraSessionImpl(keySpace, ip, port, "NONE", "cassandra", "cassandra", false);
        repo = new CassandraSampleRepository(client, ttl, registry, processors, contextConfigurations);
        if (filterForTags) {
          LOGGER.warn("FilterForTags does not work properly for NewTS!");
          searcher = new CassandraSearcher(client, registry, contextConfigurations);
        }
      }
    } catch (Exception e) {
      throw new DBException(e);
    }
  }

  /**
   * Cleanup any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void cleanup() {
    client.shutdown();
  }


  @Override
  public Status read(String metric, Long timestamp, Map<String, List<String>> tags) {
    if (metric == null || metric.equals("") || timestamp == null) {
      return Status.BAD_REQUEST;
    }
    Resource resource = new Resource("/devices/sw1/interfaces");
    int counter = 0;
    if (test) {
      return Status.OK;
    }

    if (!filterForTags || tags.size() < 1) {
      Results<Sample> samples = repo.select(Context.DEFAULT_CONTEXT, resource,
          Optional.of(org.opennms.newts.api.Timestamp.fromEpochMillis(timestamp)),
          Optional.of(org.opennms.newts.api.Timestamp.fromEpochMillis(timestamp)));
      for (Results.Row row : samples.getRows()) {
        if (row.getTimestamp().asMillis() == timestamp) {
          counter++;
        }
      }
    } else {
      // AND the smaller Querys per TAG together
      BooleanQuery mainBq = new BooleanQuery();
      for (Map.Entry<String, List<String>> tagDefinition : tags.entrySet()) {
        // OR the single Querys per TAG together
        BooleanQuery bq = new BooleanQuery();
        for (String tagvalue : tagDefinition.getValue()) {
          Query query = QueryBuilder.matchKeyAndValue(tagDefinition.getKey(), tagvalue);
          bq.add(query, Operator.OR);
        }
        mainBq.add(bq, Operator.AND);
      }
      SearchResults sr = searcher.search(Context.DEFAULT_CONTEXT, mainBq);
      if (sr.isEmpty()) {
        return Status.NOT_FOUND;
      }
      // TODO check timestamp by iterating result
    }

    if (counter == 0) {
      LOGGER.info("Found no values for metric {} for timestamp {}.", metric, timestamp);
      return Status.NOT_FOUND;
    } else if (counter > 1) {
      LOGGER.info("Found more than one value for metric {} for timestamp {}.", metric, timestamp);
    }
    return Status.OK;
  }

  @Override
  public Status scan(String metric, Long startTs, Long endTs, Map<String, List<String>> tags,
                     AggregationOperation aggreg, int timeValue, TimeUnit timeUnit) {
    if (metric == null || metric.equals("") || startTs == null || endTs == null) {
      return Status.BAD_REQUEST;
    }
    Resource resource = new Resource("/devices/sw1/interfaces");

    if (test) {
      return Status.OK;
    }
    if (filterForTags && tags.size() >= 1) {
      LOGGER.warn("FilterForTags can't work together with SCAN/AVG/SUM/COUNT for NewTS!");
      return Status.NOT_IMPLEMENTED;
    }
    // Duration for repo is resolution
    // Duration must be a divider of the interval
    // 1 is not allowed/possible
    if (timeValue != 0 && TimeUnit.MILLISECONDS.convert(timeValue, timeUnit) < 2) {
      LOGGER.warn("NewTS supports only values > 2 as resolution. Defaulting to 2.");
      timeValue = 2;
    }
    Duration duration = Duration.millis(TimeUnit.MILLISECONDS.convert(endTs - startTs, TimeUnit.MILLISECONDS) * 2);
    // we must use double duration, otherwise sampleinterval wont work.
    Duration sampleInterval = Duration.millis(TimeUnit.MILLISECONDS.convert(endTs - startTs, TimeUnit.MILLISECONDS));
    if (timeValue != 0) {
      sampleInterval = Duration.millis(TimeUnit.MILLISECONDS.convert(timeValue, timeUnit));
      if (TimeUnit.MILLISECONDS.convert(endTs - startTs, TimeUnit.MILLISECONDS) % sampleInterval.asMillis() == 0) {
        duration = Duration.millis(TimeUnit.MILLISECONDS.convert(endTs - startTs, TimeUnit.MILLISECONDS));
      } else {
        long factor = (long) Math.ceil(duration.asMillis() / (double) sampleInterval.asMillis());
        duration = Duration.millis(sampleInterval.asMillis() * factor);
      }
    }
    Duration heartbeat = Duration.millis(TimeUnit.MILLISECONDS.convert(endTs - startTs, TimeUnit.MILLISECONDS));
    // heartbeat see https://github.com/OpenNMS/newts/wiki/RestService
    // heartbeat = max. duration for aggregations?
    // sample interval = bucketsize
    // duartion + sampleInterval see https://github.com/OpenNMS/newts/wiki/JavaAPI#measurement-selects
    // duration = complete time interval (must fit at least one sample interval in it) ...
    // also named resolution which is totally wrong..
    if (aggreg != AggregationOperation.NONE) {
      // AVG;SUM;COUNT
      ResultDescriptor descriptor = null;
      if (aggreg == AggregationOperation.AVERAGE) {
        descriptor = new ResultDescriptor(sampleInterval)
            .datasource(metric, metric, heartbeat, StandardAggregationFunctions.AVERAGE)
            .export(metric);
      } else if (aggreg == SUM) {
        // There is no sum, use MAX instead
        descriptor = new ResultDescriptor(sampleInterval)
            .datasource(metric, metric, heartbeat, StandardAggregationFunctions.MAX)
            .export(metric);
      } else if (aggreg == COUNT) {
        // There is no count, use MIN instead
        descriptor = new ResultDescriptor(sampleInterval)
            .datasource(metric, metric, heartbeat, StandardAggregationFunctions.MIN)
            .export(metric);
      }
      Results<Measurement> samples = repo.select(Context.DEFAULT_CONTEXT, resource,
          Optional.of(Timestamp.fromEpochMillis(startTs)),
          Optional.of(Timestamp.fromEpochMillis(endTs)),
          descriptor, Optional.of(duration));

      if (samples == null || samples.getRows().isEmpty()) {
        return Status.NOT_FOUND;
      }
    } else {
      // SCAN
      Results<Sample> samples = repo.select(Context.DEFAULT_CONTEXT, resource,
          Optional.of(Timestamp.fromEpochMillis(startTs)),
          Optional.of(Timestamp.fromEpochMillis(endTs)));
      if (samples == null || samples.getRows().isEmpty()) {
        return Status.NOT_FOUND;
      }
    }
    return Status.OK;
  }

  @Override
  public Status insert(String metric, Long timestamp, double value, Map<String, ByteIterator> tags) {
    if (metric == null || metric.equals("") || timestamp == null) {
      return Status.BAD_REQUEST;
    }
    try {
      List<Sample> samples = new ArrayList<>();
      Map<String, String> tagMap = new HashMap<>();
      for (Map.Entry<String, ByteIterator> tag : tags.entrySet()) {
        tagMap.put(tag.getKey(), tag.getValue().toString());
      }
      samples.add(
          new Sample(
              org.opennms.newts.api.Timestamp.fromEpochMillis(timestamp),
              new Resource("/devices/sw1/interfaces"),
              metric,
              MetricType.GAUGE,
              new Gauge(value),
              tagMap
          )
      );
      repo.insert(samples);
      return Status.OK;
    } catch (Exception e) {
      LOGGER.error("Failed to process insert to metric [{}] due to {}", metric, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }
}

