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
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.List;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestOperationOrderRecorder {

  private static String outputDirectory;
  private static OperationOrderRecorder operationOrderRecorder;
  private static DiscreteGenerator discreteGenerator;

  @BeforeClass
  public static void setUp() throws IOException {
    outputDirectory = System.getProperty("user.dir") + File.separator + "test";
    FileUtils.deleteDirectory(outputDirectory);

    discreteGenerator = new DiscreteGenerator();
    discreteGenerator.addValue(1 / 4.0, "read");
    discreteGenerator.addValue(1 / 4.0, "write");
    discreteGenerator.addValue(1 / 4.0, "scan");
    discreteGenerator.addValue(1 / 4.0, "update");
  }

  @Before
  public void initOperationRecorder() throws IOException {
    operationOrderRecorder = new OperationOrderRecorder(outputDirectory, discreteGenerator);
  }

  @After
  public void clearDirectory() throws IOException {
    FileUtils.deleteDirectory(outputDirectory);
  }

  @Test
  public void checkFilesCreated() {
    File file = new File(outputDirectory);

    assertEquals(1, Objects.requireNonNull(file.list()).length);
  }

  @Test(expected = IOException.class)
  public void checkFilesAlreadyPresent() throws IOException {
    operationOrderRecorder = new OperationOrderRecorder(outputDirectory, discreteGenerator);
  }

  @Test
  public void write100Operations() throws IOException {
    int numberOfOperations = 100;

    for (int i = 0; i < numberOfOperations; i++) {
      operationOrderRecorder.nextValue();
    }

    File file = new File(outputDirectory, "operations.txt");
    List<String> operations = Files.readAllLines(file.toPath(), Charset.forName(new FileReader(file).getEncoding()));

    assertEquals(numberOfOperations, operations.size());
  }

  @Test
  public void testLastValue() {
    assertNull(operationOrderRecorder.lastValue());

    String operation = operationOrderRecorder.nextValue();

    assertEquals(operation, operationOrderRecorder.lastValue());

  }
}