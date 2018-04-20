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

import java.util.ArrayList;
import java.util.List;

/**
 * Graph for the graph workload.
 */
public class Graph {
  private List<Node> nodes;
  private List<Edge> edges;

  Graph() {
    nodes = new ArrayList<>();
    edges = new ArrayList<>();
  }

  public List<Node> getNodes() {
    return nodes;
  }

  public List<Edge> getEdges() {
    return edges;
  }

  /**
   * Adds a {@link Node} the the list of nodes.
   *
   * @param node to add.
   */
  public void addNode(Node node) {
    this.nodes.add(node);
  }

  /**
   * Adds a {@link Edge} the the list of edges.
   *
   * @param edge to add.
   */
  public void addEdge(Edge edge) {
    edges.add(edge);
  }
}