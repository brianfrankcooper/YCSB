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
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ListIterator;

/**
 * Class to recreate the saved value produced by a {@link RandomGraphComponentRecorder}, stored in a file in the
 * given directory.
 */
public class RandomGraphComponentRecreator extends RandomGraphComponentGenerator {

  private ListIterator<String> nodeIterator;
  private ListIterator<String> edgeIterator;
  private ListIterator<String> componentIterator;


  RandomGraphComponentRecreator(String inputDirectory,
                                GraphDataGenerator graphDataGenerator) throws IOException {
    super(inputDirectory, graphDataGenerator);

    nodeIterator = getIterator(getNodeFile());
    edgeIterator = getIterator(getEdgeFile());
    componentIterator = getIterator(getComponentFile());
  }

  @Override
  public String getExceptionMessage() {
    return "Random graph component files are not present.";
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

  @Override
  long chooseRandomNodeId() {
    return Long.parseLong(getNextValue(nodeIterator));
  }

  @Override
  long chooseRandomEdgeId() {
    return Long.parseLong(getNextValue(edgeIterator));
  }

  @Override
  RandomComponent randomNodeOrEdge() {
    return RandomComponent.getRandomComponent(getNextValue(componentIterator));
  }

  private ListIterator<String> getIterator(File file) throws IOException {
    return Files.readAllLines(file.toPath(), Charset.forName(new FileReader(file)
        .getEncoding())).listIterator();
  }

  private String getNextValue(ListIterator<String> iterator) {
    if (iterator.hasNext()) {
      return iterator.next();
    }

    return "";
  }
}
