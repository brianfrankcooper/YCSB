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

import java.util.Map;
import java.util.Set;

/**
 * Class to extract common fields and methods from {@link Node}s and {@link Edge}s.
 */
public abstract class GraphComponent {
  public static final String ID_IDENTIFIER = "id";
  public static final String LABEL_IDENTIFIER = "label";
  private final long id;
  private final String label;

  GraphComponent(String label, long id) {
    this.id = id;
    this.label = label;
  }

  public final long getId() {
    return id;
  }

  final String getLabel() {
    return label;
  }

  public abstract String getComponentTypeIdentifier();

  public abstract Map<String, ByteIterator> getHashMap();

  public abstract Set<String> getFieldSet();
}
