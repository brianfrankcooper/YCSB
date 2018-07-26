/*
 * Copyright (c) 2018 YCSB contributors. All rights reserved.
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

package com.yahoo.ycsb.generator.graph;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Edge for the graph in the graph workload.
 */
public class Edge extends GraphComponent {
  public static final String EDGE_IDENTIFIER = "Edge";
  public static final String START_IDENTIFIER = "start";
  public static final String END_IDENTIFIER = "end";
  public static final Set<String> EDGE_FIELDS_SET = new HashSet<>();
  private static long edgeIdCount = 0;

  static {
    EDGE_FIELDS_SET.add(ID_IDENTIFIER);
    EDGE_FIELDS_SET.add(LABEL_IDENTIFIER);
    EDGE_FIELDS_SET.add(START_IDENTIFIER);
    EDGE_FIELDS_SET.add(END_IDENTIFIER);
  }

  private long startNodeId;
  private long endNodeId;

  Edge(String label, long startNodeId, long endNodeId) {
    super(getAndIncrementIdCounter(), label);

    this.startNodeId = startNodeId;
    this.endNodeId = endNodeId;
  }

  private Edge(long id, String label, long startNodeId, long endNodeId) {
    super(id, label);
    this.startNodeId = startNodeId;
    this.endNodeId = endNodeId;
  }

  /**
   * @return the number of edges created. Note that the id of the last created {@link Edge} is one smaller since we
   * count from 0.
   */
  public static long getEdgeCount() {
    return edgeIdCount;
  }

  private static synchronized long getAndIncrementIdCounter() {
    return edgeIdCount++;
  }

  Edge copyEdge() {
    return new Edge(this.getId(), this.getLabel(), this.startNodeId, this.endNodeId);
  }

  @Override
  public String getComponentTypeIdentifier() {
    return EDGE_IDENTIFIER;
  }

  @Override
  public Map<String, ByteIterator> getHashMap() {
    HashMap<String, ByteIterator> values = new HashMap<>();

    values.put(ID_IDENTIFIER, new StringByteIterator(String.valueOf(this.getId())));
    values.put(LABEL_IDENTIFIER, new StringByteIterator(this.getLabel()));
    values.put(START_IDENTIFIER, new StringByteIterator(String.valueOf(startNodeId)));
    values.put(END_IDENTIFIER, new StringByteIterator(String.valueOf(endNodeId)));
    return values;
  }

  @Override
  public Set<String> getFieldSet() {
    return EDGE_FIELDS_SET;
  }

  long getStartNodeId() {
    return startNodeId;
  }

  long getEndNodeId() {
    return endNodeId;
  }
}