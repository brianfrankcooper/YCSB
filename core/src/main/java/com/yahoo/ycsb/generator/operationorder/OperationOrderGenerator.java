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
import com.yahoo.ycsb.generator.StoringGenerator;

import java.io.File;
import java.io.IOException;

/**
 * This class generates {@link String}s which represent operations for a {@link com.yahoo.ycsb.DB}.
 * The value of that String is saves in a operations.txt file for later reproduction.
 */
public abstract class OperationOrderGenerator extends StoringGenerator<String> {

  private static final String OPERATION_FILE_NAME = "operations.txt";
  private static final String CLASS_NAME = OperationOrderGenerator.class.getSimpleName();
  private final File operationFile;
  private String lastOperation;

  OperationOrderGenerator(String outputDirectory) throws IOException {
    File directory = new File(outputDirectory);
    operationFile = new File(outputDirectory, OPERATION_FILE_NAME);

    if (!checkFiles(directory, operationFile)) {
      throw new IOException(getExceptionMessage());
    }
  }

  /**
   * Creates a {@link OperationOrderRecorder} or a {@link OperationOrderRecreator} depending on the given values.
   *
   * @param directory          which contains the recorded data or where the data will be recorded to.
   * @param isRunPhase         tells the current execution phase (load or run).
   * @param operationGenerator passed to the {@link OperationOrderRecorder} constructor to get the values to return.
   * @return a subclass of the {@link OperationOrderGenerator} or null if it's not the run phase (during load
   * this is not needed).
   * @throws IOException if an I/O exception occurs.
   */
  public static OperationOrderGenerator create(String directory,
                                               boolean isRunPhase,
                                               DiscreteGenerator operationGenerator) throws IOException {
    if (isRunPhase) {
      if (checkDataPresentAndCleanIfSomeMissing(CLASS_NAME, new File(directory, OPERATION_FILE_NAME))) {
        System.out.println(CLASS_NAME + " creating RECREATOR.");
        return new OperationOrderRecreator(directory);
      } else {
        System.out.println(CLASS_NAME + " creating RECORDER.");
        return new OperationOrderRecorder(directory, operationGenerator);
      }
    } else {
      System.out.println(CLASS_NAME + " not needed during load phase. Nothing created.");
      return null;
    }
  }

  @Override
  public final String lastValue() {
    return lastOperation;
  }

  String getLastOperation() {

    return lastOperation;
  }

  void setLastOperation(String operation) {
    this.lastOperation = operation;
  }

  final File getOperationFile() {
    return operationFile;
  }
}
