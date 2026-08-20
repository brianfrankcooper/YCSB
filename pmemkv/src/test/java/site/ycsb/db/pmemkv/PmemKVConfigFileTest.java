/*
 * Copyright (c) 2021 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db;

import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.util.*;

public class PmemKVConfigFileTest {

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  private PmemKVClient instance;

  /* Setup dummy config params and check if DB opens properly.
   * It also checks if blackhole can be run without path and size. */
  @Test
  public void dummyTest() throws Exception {
    instance = new PmemKVClient();

    final Properties properties = new Properties();
    properties.setProperty(PmemKVClient.ENGINE_PROPERTY, "blackhole");
    properties.setProperty(PmemKVClient.JSON_CONFIG_PROPERTY,
        PmemKVClient.class.getClassLoader().getResource("config_params.json").getPath());
    instance.setProperties(properties);

    instance.init();
    instance.cleanup();
  }

  /* Setup dummy config params and check all engines */
  @Test
  public void enginesTest() throws Exception {
    instance = new PmemKVClient();

    ArrayList<String> engines = new ArrayList(4);
    engines.add("cmap");
    engines.add("stree");
    ArrayList<String> enginesWithDir = new ArrayList(2);
    enginesWithDir.add("vsmap");
    enginesWithDir.add("vcmap");
    engines.addAll(enginesWithDir);

    for (String engine : engines) {
      String db_file = "";
      final Properties properties = new Properties();
      properties.setProperty(PmemKVClient.ENGINE_PROPERTY, engine);
      if (enginesWithDir.contains(engine)) {
        db_file = "";
      } else {
        db_file = "/pmemkv_db";
      }
      properties.setProperty(PmemKVClient.PATH_PROPERTY,
          tmpFolder.newFolder().getAbsolutePath() + db_file);
      properties.setProperty(PmemKVClient.SIZE_PROPERTY, "33554432");
      properties.setProperty(PmemKVClient.JSON_CONFIG_PROPERTY,
          PmemKVClient.class.getClassLoader().getResource("config_params.json").getPath());
      instance.setProperties(properties);

      instance.init();
      instance.cleanup();
    }
  }
}
