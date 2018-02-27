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
import org.opennms.newts.cassandra.ContextConfigurations;
import org.opennms.newts.cassandra.search.CassandraSearcher;
import org.opennms.newts.persistence.cassandra.CassandraSampleRepository;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

  private String ip = "localhost";
  private int port = 9042;
  private String keySpace = "newts";

  private CassandraSession client;
  private SampleRepository repo;
  private CassandraSearcher searcher;
  private ContextConfigurations cc = new ContextConfigurations();
  private int heartbeatFactor = 10; // heartbeat = resolution * heartbeatfactor
  // NewTS can only filter for tags OR for time, tags + avg/sum/count/scan does not work
  private boolean filterForTags = false;

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    super.init();
    try {
      if (!getProperties().containsKey("port") && !test) {
        throw new DBException("No port given, abort.");
      }
      port = Integer.parseInt(getProperties().getProperty("port", String.valueOf(port)));
      if (!getProperties().containsKey("ip") && !test) {
        throw new DBException("No ip given, abort.");
      }
      ip = getProperties().getProperty("ip", ip);
      keySpace = getProperties().getProperty("keyspace", keySpace);
      heartbeatFactor = Integer.parseInt(getProperties().getProperty("heartbeatfactor", "10"));
      filterForTags = Boolean.parseBoolean(getProperties().getProperty("filterForTags", "false"));
      if (debug) {
        System.out.println("The following properties are given: ");
        for (String element : getProperties().stringPropertyNames()) {
          System.out.println(element + ": " + getProperties().getProperty(element));
        }
      }
      if (!test) {
        SampleProcessorService processors = new SampleProcessorService(32);
        int ttl = 86400 * 365; // 1 Jahr TTL
        MetricRegistry registry = new MetricRegistry();
        // keypsace,ip,port,compression,user,pw
        this.client = new CassandraSession(keySpace, this.ip, this.port, "NONE", "cassandra", "cassandra");
        this.repo = new CassandraSampleRepository(this.client, ttl, registry, processors, this.cc);
        if (filterForTags) {
          System.err.println("WARNING: FilterForTags does not work properly for NewTS!");
          this.searcher = new CassandraSearcher(this.client, registry, this.cc);
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
  public void cleanup() throws DBException {
    this.client.shutdown();
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

    if (!this.filterForTags || tags.size() < 1) {
      Results<Sample> samples = this.repo.select(Context.DEFAULT_CONTEXT, resource,
          Optional.of(org.opennms.newts.api.Timestamp.fromEpochMillis(timestamp)),
          Optional.of(org.opennms.newts.api.Timestamp.fromEpochMillis(timestamp)));
      for (Results.Row row : samples.getRows()) {

        if (row.getTimestamp().asMillis() == timestamp) {
          counter++;
        }
      }
    } else {
      BooleanQuery mainBq = new BooleanQuery(); // AND the smaller Querys per TAG together
      for (String tag : tags.keySet()) {
        BooleanQuery bq = new BooleanQuery(); // OR the single Querys per TAG together
        for (String tagvalue : tags.get(tag)) {
          Query query = QueryBuilder.matchKeyAndValue(tag, tagvalue);
          bq.add(query, Operator.OR);
        }
        mainBq.add(bq, Operator.AND);
      }
      SearchResults sr = this.searcher.search(Context.DEFAULT_CONTEXT, mainBq);
      if (!sr.isEmpty()) {
        SearchResults.Result res;
        while (sr.iterator().hasNext()) {
          res = sr.iterator().next();
          // Untested -> Missing: Check if timestamp is there
        }
      } else {
        return Status.NOT_FOUND;
      }
    }

    if (counter == 0) {
      System.err.println("ERROR: Found no values for metric: " + metric + " for timestamp: " + timestamp + ".");
      return Status.NOT_FOUND;
    } else if (counter > 1) {
      System.err.println("ERROR: Found more than one value for metric: " + metric + " for timestamp: "
          + timestamp + ".");
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
    if (!this.filterForTags || tags.size() < 1) {
      // Duration for repo is resolution
      // Duration must be a divider of the interval
      // 1 is not allowed/possible
      if (timeValue != 0 && timeUnit.MILLISECONDS.convert(timeValue, timeUnit) < 2) {
        System.err.println("WARNING: NewTS supports only values > 2 as resolution. Defaulting to 2.");
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
          long factor = (long) Math.ceil(duration.asMillis() / sampleInterval.asMillis());
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
        Results<Measurement> samples = this.repo.select(Context.DEFAULT_CONTEXT, resource,
            Optional.of(org.opennms.newts.api.Timestamp.fromEpochMillis(startTs)),
            Optional.of(org.opennms.newts.api.Timestamp.fromEpochMillis(endTs)),
            descriptor, Optional.of(duration));

        if (samples == null || samples.getRows().size() == 0) {
          return Status.NOT_FOUND;
        }
      } else {
        // SCAN
        Results<Sample> samples = this.repo.select(Context.DEFAULT_CONTEXT, resource,
            Optional.of(org.opennms.newts.api.Timestamp.fromEpochMillis(startTs)),
            Optional.of(org.opennms.newts.api.Timestamp.fromEpochMillis(endTs)));
        if (samples == null || samples.getRows().size() == 0) {
          return Status.NOT_FOUND;
        }
      }
    } else {
      System.err.println("WARNING: FilterForTags can't work together with SCAN/AVG/SUM/COUNT for NewTS!");
      return Status.NOT_IMPLEMENTED;
    }
    return Status.OK;
  }

  @Override
  public Status insert(String metric, Long timestamp, double value, Map<String, ByteIterator> tags) {
    if (metric == null || metric.equals("") || timestamp == null) {
      return Status.BAD_REQUEST;
    }
    try {
      List<Sample> samples = new ArrayList<Sample>();
      Map<String, String> tagMap = new HashMap<String, String>();
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
      this.repo.insert(samples);
      return Status.OK;
    } catch (Exception e) {
      System.err.println("ERROR: Error in processing insert to metric: " + metric + e);
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }
}

