/*
 * Copyright (c) 2015 - 2018 Andreas Bader, 2018 YCSB Contributors All rights reserved.
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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.google.common.collect.Maps;
import com.pardot.rhombus.CassandraConfiguration;
import com.pardot.rhombus.ConnectionManager;
import com.pardot.rhombus.Criteria;
import com.pardot.rhombus.ObjectMapper;
import com.pardot.rhombus.cobject.CKeyspaceDefinition;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.TimeseriesDB;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Rhombusclient for YCSB framework.
 * Creates its cassandra keyspace at init()
 * Takes more than one ip
 * No AVG,SUM -> Using COUNT instead
 * (No use of any granularity/resolution)
 * No OR/AND -> Filtering for Tags works very poor
 */
public class RhombusClient extends TimeseriesDB {

  private String ip = "localhost";
  private String dataCenter = "datacenter1";
  private boolean filterForTags = true;
  private String keySpaceDefinitionFile = "/home/vagrant/files/rhombus_repl1.json";

  private ConnectionManager client;
  private ObjectMapper om;

  private String[] usedTags = {"TAG0", "TAG1", "TAG2"};
  private String alwaysSetTagName = "TAGALWAYSSET1";

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    super.init();
    if (debug) {
      Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
      root.setLevel(Level.DEBUG);
    } else {
      Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
      // actually we want warning and above to be logged either way
      root.setLevel(Level.WARN);
    }
    if (debug) {
      LOGGER.info("The following properties are given: ");
      for (String element : getProperties().stringPropertyNames()) {
        LOGGER.info("{}: {}", element, getProperties().getProperty(element));
      }
    }
    ip = getProperties().getProperty("ip", ip);
    keySpaceDefinitionFile = getProperties().getProperty("keySpacedefinitionfile", keySpaceDefinitionFile);
    dataCenter = getProperties().getProperty("datacenter", dataCenter);
    filterForTags = Boolean.parseBoolean(getProperties().getProperty("filterForTags",
        Boolean.toString(filterForTags)));
    try {
      if (!test) {
        if (!getProperties().containsKey("ip")) {
          throw new DBException("No ip given, abort.");
        }
        CassandraConfiguration config = new CassandraConfiguration();
        config.setContactPoints(Arrays.asList(ip.split(",")));
        config.setLocalDatacenter(dataCenter);
        client = new ConnectionManager(config);
        client.buildCluster();
        final CKeyspaceDefinition keyspaceDefinition = CKeyspaceDefinition.fromJsonFile(keySpaceDefinitionFile);
        // false = do not rebuild if exist
        client.buildKeyspace(keyspaceDefinition, false);
        client.setDefaultKeyspace(keyspaceDefinition);
        om = client.getObjectMapper();
        if (debug) {
          client.setLogCql(true);
          om.setLogCql(true);
        } else {
          client.setLogCql(false);
          om.setLogCql(false);
        }
      }
    } catch (Exception e) {
      throw new DBException(e);
    }

  }

  @Override
  public void cleanup() {
    client.teardown();
  }

  @Override
  public Status read(String metric, Long timestamp, Map<String, List<String>> tags) {
    if (metric == null || metric.equals("") || timestamp == null || tags == null) {
      return Status.BAD_REQUEST;
    }
    if (tags.keySet().size() > usedTags.length) {
      LOGGER.warn("More tags used than configured in Rhombus. Please adjust usedTags and rhombus.json, " +
          "or use only {} tags.", usedTags.length);
    }
    try {
      int counter = 0;
      Criteria criteria = new Criteria();
      SortedMap<String, Object> values = Maps.newTreeMap();
      if (filterForTags && !tags.isEmpty()) {
        // FIXME THIS IS NOT HOW MAPS WORK, FFS! WHY WAS THE ORIGINAL CODE EQUIVALENT TO THIS?
        tags.forEach((tagName, tagValues) -> tagValues.forEach(tagValue -> values.put(tagName, tagValue)));
      } else {
        values.put(alwaysSetTagName, 1L);
      }
      criteria.setIndexKeys(values);
      criteria.setStartTimestamp(timestamp);
      criteria.setEndTimestamp(timestamp);
      if (test) {
        return Status.OK;
      }
      List<Map<String, Object>> results = om.list(metric, criteria);
      // TODO: this assumes that results are all that we wanted...
      counter = results.size();
      if (counter == 0) {
        LOGGER.info("Found no values for metric {} at timestamp {}", metric, timestamp);
        return Status.NOT_FOUND;
      } else if (counter > 1) {
        LOGGER.warn("Found more than one value for metric {} at timestamp {}", metric, timestamp);
      }
      return Status.OK;
    } catch (Exception e) {
      LOGGER.error("Failed to process READ for metric {} due to {}", metric, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String metric, Long startTs, Long endTs, Map<String, List<String>> tags,
                     AggregationOperation aggreg, int timeValue, TimeUnit timeUnit) {
    if (metric == null || metric.equals("") || startTs == null || endTs == null || tags == null) {
      return Status.BAD_REQUEST;
    }
    if (tags.keySet().size() > usedTags.length) {
      LOGGER.warn("More tags used than configured in Rhombus. Please adjust usedTags and rhombus.json, " +
          "or use only {} tags.", usedTags.length);
    }
    if (timeValue != 0) {
      LOGGER.warn("Rhombus does not support granularities.");
    }
    try {
      Criteria criteria = new Criteria();
      SortedMap<String, Object> values = Maps.newTreeMap();
      if (filterForTags && !tags.isEmpty()) {
        // FIXME still not how maps work ...
        tags.forEach((tagName, tagValues) -> tagValues.forEach(value -> values.put(tagName, value)));
      } else {
        values.put(alwaysSetTagName, 1L);
      }
      criteria.setStartTimestamp(startTs);
      criteria.setEndTimestamp(endTs);
      criteria.setIndexKeys(values);
      if (test) {
        return Status.OK;
      }
      if (aggreg != AggregationOperation.NONE) {
        // Every Count result is okay, ERROR is only required in case of an exception
        om.count(metric, criteria);
      } else {
        List<Map<String, Object>> results = om.list(metric, criteria);
        if (results == null || results.isEmpty()) {
          return Status.NOT_FOUND;
        }
      }
    } catch (Exception e) {
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status insert(String metric, Long timestamp, double value, Map<String, ByteIterator> tags) {
    if (metric == null || metric.equals("") || timestamp == null) {
      return Status.BAD_REQUEST;
    }
    try {
      Map<String, Object> data = new HashMap<>();
      data.put("value", value);
      // new is needed, otherwise .remove does not work, see http://stackoverflow.com/q/6260113/
      List<String> tagList = new ArrayList<>(Arrays.asList(usedTags.clone()));
      for (Map.Entry<String, ByteIterator> tag : tags.entrySet()) {
        data.put(tag.getKey(), tag.getValue().toString());
        tagList.remove(tag.getKey());
      }
      for (String unusedTag : tagList) {
        data.put(unusedTag, "");
      }
      data.put(alwaysSetTagName, 1L);
      om.insert(metric, data, timestamp);
      return Status.OK;
    } catch (Exception e) {
      LOGGER.error("Failed to process insert to metric {} due to {}", metric, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }
}

