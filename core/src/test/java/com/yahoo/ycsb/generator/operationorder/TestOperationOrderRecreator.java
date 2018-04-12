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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestOperationOrderRecreator {

  private static String inputDirectory = "src"
      + File.separator
      + "test"
      + File.separator
      + "resources";
  private static OperationOrderRecreator operationOrderRecreator;

  @Before
  public void setUp() throws IOException {
    operationOrderRecreator = new OperationOrderRecreator(inputDirectory);
  }

  @Test
  public void loadFile() throws IOException {
    operationOrderRecreator = new OperationOrderRecreator(inputDirectory);
  }

  @Test(expected = IOException.class)
  public void loadDirectoryWithoutNecessaryFile() throws IOException {
    operationOrderRecreator = new OperationOrderRecreator(System.getProperty("user.dir"));
  }

  @Test
  public void readLines() throws IOException {
    File file = new File(inputDirectory, "operations.txt");
    List<String> operations = Files.readAllLines(file.toPath(), Charset.forName(new FileReader(file).getEncoding()));

    for (String operation : operations) {
      String actual = operationOrderRecreator.nextValue();
      Assert.assertEquals(operation, actual);
    }
  }

  @Test
  public void testLastValue() {
    assertNull(operationOrderRecreator.lastValue());

    String operation = operationOrderRecreator.nextValue();

    assertEquals(operation, operationOrderRecreator.lastValue());

  }

}