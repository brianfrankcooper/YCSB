/*
  Copyright (c) 2018 YCSB contributors. All rights reserved.
  <p>
  Licensed under the Apache License, Version 2.0 (the "License"); you
  may not use this file except in compliance with the License. You
  may obtain a copy of the License at
  <p>
  http://www.apache.org/licenses/LICENSE-2.0
  <p>
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
  implied. See the License for the specific language governing
  permissions and limitations under the License. See accompanying
  LICENSE file.
 */

package com.yahoo.ycsb.generator.graph;

import com.google.gson.stream.JsonReader;

import java.io.*;
import java.util.Properties;

/**
 * This class takes a data set of graph data and reproduces it.
 */
public class GraphDataRecreator extends GraphDataGenerator {

  private final BufferedReader bufferedReader;
  private final BufferedReader loadReader;

  GraphDataRecreator(String inputDirectory, boolean isRunPhase, Properties properties) throws IOException {
    super(inputDirectory, isRunPhase, properties);

    bufferedReader = new BufferedReader(new FileReader(getGraphFile()));

    if (isRunPhase && getLoadFile(inputDirectory).exists()) {
      loadReader = new BufferedReader(new FileReader(getLoadFile(inputDirectory)));
    } else {
      loadReader = null;
    }
  }

  @Override
  Graph getNextLoadGraph() {
    try {
      if (loadReader != null) {
        return getNextGraphFromReader(loadReader);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    return new Graph();
  }

  @Override
  Graph createNextValue() throws IOException {
    return getNextGraphFromReader(bufferedReader);
  }

  @Override
  public String getExceptionMessage() {
    return "Graph data files aren't present.";
  }

  @Override
  public boolean checkFiles(File directory, File... files) {
    boolean directoryPresent = directory.exists() && directory.isDirectory();
    boolean filesCreated = true;

    for (File file : files) {
      filesCreated = filesCreated && file.exists();
    }

    return directoryPresent && filesCreated;
  }

  private Graph getNextGraphFromReader(BufferedReader reader) throws IOException {
    String line = reader.readLine();
    if (line != null) {
      JsonReader jsonReader = new JsonReader(new StringReader(line));
      return getGson().fromJson(jsonReader, getValueType());
    } else {
      return new Graph();
    }
  }
}