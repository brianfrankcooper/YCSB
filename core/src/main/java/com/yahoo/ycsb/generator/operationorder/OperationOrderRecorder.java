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

package com.yahoo.ycsb.generator.operationorder;

import com.yahoo.ycsb.generator.DiscreteGenerator;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * This class generates {@link String}s which represent operations for a {@link com.yahoo.ycsb.DB}.
 * The value of that string is saves in a operations.txt file for later reproduction.
 */
public class OperationOrderRecorder extends OperationOrderGenerator {

  private FileWriter fileWriter;
  private DiscreteGenerator discreteGenerator;

  /**
   * @param outputDirectory   for the operations.txt file to be stored.
   * @param discreteGenerator to generate the values to return and store.
   * @throws IOException if something is wrong with the output file/directory.
   */
  OperationOrderRecorder(String outputDirectory, DiscreteGenerator discreteGenerator) throws IOException {
    super(outputDirectory);

    fileWriter = new FileWriter(getOperationFile(), true);
    this.discreteGenerator = discreteGenerator;
  }

  @Override
  public String nextValue() {
    setLastOperation(discreteGenerator.nextValue());

    try {
      fileWriter.write(getLastOperation());
      fileWriter.write("\n");
      fileWriter.flush();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return getLastOperation();
  }

  @Override
  public String getExceptionMessage() {
    return "Could not create operation order file or it already present.";
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
}
