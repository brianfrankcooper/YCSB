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

package com.yahoo.ycsb.generator.graph.randomcomponents;

import com.yahoo.ycsb.generator.graph.GraphDataGenerator;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/**
 * Class to pick and store a random GraphComponent (Node or Edge).
 */
public class RandomGraphComponentRecorder extends RandomGraphComponentGenerator {

  private FileWriter nodeFileWriter;
  private FileWriter edgeFileWriter;
  private FileWriter componentFileWriter;
  private Random random;

  RandomGraphComponentRecorder(String outputDirectory,
                               GraphDataGenerator graphDataGenerator) throws IOException {
    super(outputDirectory, graphDataGenerator);

    nodeFileWriter = new FileWriter(getNodeFile());
    edgeFileWriter = new FileWriter(getEdgeFile());
    componentFileWriter = new FileWriter(getComponentFile());

    this.random = new Random();
  }

  @Override
  public String getExceptionMessage() {
    return "Could not create random graph component files or they are already present.";
  }

  @Override
  public boolean checkFiles(File directory, File... files) throws IOException {
    boolean directoryPresent = directory.exists() || directory.mkdirs();
    boolean filesCreated = true;

    for (File file : files) {
      filesCreated = filesCreated && file.createNewFile();
    }

    return directoryPresent && filesCreated;
  }

  @Override
  long chooseRandomNodeId() {
    int lastNodeId = (int) getGraphDataGenerator().getLastNodeId();
    int maxBound = lastNodeId == 0 ? 1 : lastNodeId;

    int id = random.nextInt(maxBound);

    try {
      writeLine(nodeFileWriter, String.valueOf(id));
    } catch (IOException e) {
      e.printStackTrace();
    }

    return id;
  }

  @Override
  long chooseRandomEdgeId() {
    int lastEdgeId = (int) getGraphDataGenerator().getLastEdgeId();
    int maxBound = lastEdgeId == 0 ? 1 : lastEdgeId;

    int id = random.nextInt(maxBound);

    try {
      writeLine(edgeFileWriter, String.valueOf(id));
    } catch (IOException e) {
      e.printStackTrace();
    }

    return id;
  }

  @Override
  RandomComponent randomNodeOrEdge() {
    RandomComponent id = RandomComponent.values()[random.nextInt(2)];

    try {
      writeLine(componentFileWriter, id.name());
    } catch (IOException e) {
      e.printStackTrace();
    }

    return id;
  }

  private void writeLine(FileWriter fileWriter, String value) throws IOException {
    fileWriter.write(value);
    fileWriter.write("\n");
    fileWriter.flush();
  }
}
