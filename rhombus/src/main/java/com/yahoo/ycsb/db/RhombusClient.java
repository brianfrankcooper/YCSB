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

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
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

  static String readFile(String path, Charset encoding)
      throws IOException {
    byte[] encoded = Files.readAllBytes(Paths.get(path));
    return new String(encoded, encoding);
  }

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
      System.out.println("The following properties are given: ");
      for (String element : getProperties().stringPropertyNames()) {
        System.out.println(element + ": " + getProperties().getProperty(element));
      }
    }
    try {
      if (!getProperties().containsKey("ip") && !test) {
        throw new DBException("No ip given, abort.");
      }
      ip = getProperties().getProperty("ip", ip);
      keySpaceDefinitionFile = getProperties().getProperty("keySpacedefinitionfile", keySpaceDefinitionFile);
      dataCenter = getProperties().getProperty("datacenter", dataCenter);
      filterForTags = Boolean.parseBoolean(getProperties().getProperty("filterForTags",
          Boolean.toString(filterForTags)));
      if (!test) {
        CassandraConfiguration config = new CassandraConfiguration();
        List contactPoints = new ArrayList();
        for (String splittedIp : ip.split(",")) {
          contactPoints.add(splittedIp);
        }
        config.setContactPoints(contactPoints);
        config.setLocalDatacenter(dataCenter);
        this.client = new ConnectionManager(config);
        this.client.buildCluster();
        String json = "";
        try {
          json = readFile(keySpaceDefinitionFile, Charset.forName("UTF-8"));
        } catch (Exception e) {
          throw new DBException("Can't open or read " + keySpaceDefinitionFile + ". " + e);
        }
        CKeyspaceDefinition keyspaceDefinition = CKeyspaceDefinition.fromJsonString(json);
        // false = do not rebuild if exist
        this.client.buildKeyspace(keyspaceDefinition, false);
        this.client.setDefaultKeyspace(keyspaceDefinition);
        this.om = this.client.getObjectMapper();
        if (debug) {
          this.client.setLogCql(true);
          this.om.setLogCql(true);
        } else {
          this.client.setLogCql(false);
          this.om.setLogCql(false);
        }
      }
    } catch (Exception e) {
      throw new DBException(e);
    }

  }

  @Override
  public void cleanup() throws DBException {
    this.client.teardown();
  }

  @Override
  public Status read(String metric, Long timestamp, Map<String, List<String>> tags) {
    if (metric == null || metric.equals("") || timestamp == null) {
      return Status.BAD_REQUEST;
    }
    if (tags != null && tags.keySet().size() > usedTags.length) {
      System.err.println("WARNING: More tags used than configured in Rhombus. " +
          "Please adjust usedTags and rhombus.json! Or use only " + usedTags.length + " tags.");
    }
    try {
      int counter = 0;
      Criteria criteria = new Criteria();
      SortedMap values = Maps.newTreeMap();
      if (this.filterForTags && tags.size() > 0) {
        for (String tag : tags.keySet()) {
          for (String tagvalue : tags.get(tag)) {
            values.put(tag, tagvalue);
          }
        }
      } else {
        values.put(this.alwaysSetTagName, 1L);
      }
      criteria.setIndexKeys(values);
      criteria.setStartTimestamp(timestamp);
      criteria.setEndTimestamp(timestamp);
      if (test) {
        return Status.OK;
      }
      List<Map<String, Object>> results = this.om.list(metric, criteria);
      // every result will have the right timestamp, we can't check that
      for (Map<String, Object> map : results) {
        counter++;
      }

      if (counter == 0) {
        System.err.println("ERROR: Found no values for metric: " + metric + " for timestamp: " + timestamp + ".");
        return Status.NOT_FOUND;
      } else if (counter > 1) {
        System.err.println("ERROR: Found more than one value for metric: "
            + metric + " for timestamp: " + timestamp + ".");
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println("ERROR: Error while processing READ for metric: " + metric + e);
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String metric, Long startTs, Long endTs, Map<String, List<String>> tags,
                     AggregationOperation aggreg, int timeValue, TimeUnit timeUnit) {
    if (metric == null || metric.equals("") || startTs == null || endTs == null) {
      return Status.BAD_REQUEST;
    }
    if (tags != null && tags.keySet().size() > usedTags.length) {
      System.err.println("WARNING: More tags used than configured in Rhombus. " +
          "Please adjust usedTags and rhombus.json! Or use only " + usedTags.length + " tags.");
    }
    if (timeValue != 0) {
      System.err.println("WARNING: Rhombus does not support granularities.");
    }
    try {
      Criteria criteria = new Criteria();
      SortedMap values = Maps.newTreeMap();
      if (this.filterForTags && tags.size() > 0) {
        for (String tag : tags.keySet()) {
          for (String tagvalue : tags.get(tag)) {
            values.put(tag, tagvalue);
          }
        }
      } else {
        values.put(this.alwaysSetTagName, 1L);
      }
      criteria.setStartTimestamp(startTs);
      criteria.setEndTimestamp(endTs);
      criteria.setIndexKeys(values);
      if (test) {
        return Status.OK;
      }
      if (aggreg != AggregationOperation.NONE) {
        long result = this.om.count(metric, criteria);
        // Every Count result is okay, -1 is only dropped in catch
      } else {
        List<Map<String, Object>> results = this.om.list(metric, criteria);
        if (results == null || results.size() == 0) {
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
      List<String> tagList = new ArrayList<>(Arrays.asList(this.usedTags.clone()));
      for (Map.Entry<String, ByteIterator> tag : tags.entrySet()) {
        data.put(tag.getKey(), tag.getValue().toString());
        tagList.remove(tag.getKey());
      }
      for (String unusedTag : tagList) {
        data.put(unusedTag, "");
      }
      data.put(this.alwaysSetTagName, 1L);
      Object id = om.insert(metric, data, timestamp);
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

