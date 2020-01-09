/*
 * Copyright (c) 2019 YCSB contributors. All rights reserved.
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

package site.ycsb.db.rocksdb;

import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.*;

import java.util.*;

import static org.junit.Assert.assertEquals;

public class RocksDBOptionsFileTest {

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  private RocksDBClient instance;

  @Test
  public void loadOptionsFromFile() throws Exception {
    final String optionsPath = RocksDBClient.class.getClassLoader().getResource("testcase.ini").getPath();
    final String dbPath = tmpFolder.getRoot().getAbsolutePath();

    initDbWithOptionsFile(dbPath, optionsPath);
    checkOptions(dbPath);
  }

  private void initDbWithOptionsFile(final String dbPath, final String optionsPath) throws Exception {
    instance = new RocksDBClient();

    final Properties properties = new Properties();
    properties.setProperty(RocksDBClient.PROPERTY_ROCKSDB_DIR, dbPath);
    properties.setProperty(RocksDBClient.PROPERTY_ROCKSDB_OPTIONS_FILE, optionsPath);
    instance.setProperties(properties);

    instance.init();
    instance.cleanup();
  }

  private void checkOptions(final String dbPath) throws Exception {
    final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
    final DBOptions dbOptions = new DBOptions();

    RocksDB.loadLibrary();
    OptionsUtil.loadLatestOptions(dbPath, Env.getDefault(), dbOptions, cfDescriptors);

    try {
      assertEquals(dbOptions.walSizeLimitMB(), 42);

      // the two CFs should be "default" and "usertable"
      assertEquals(cfDescriptors.size(), 2);
      assertEquals(cfDescriptors.get(0).getOptions().ttl(), 42);
      assertEquals(cfDescriptors.get(1).getOptions().ttl(), 42);
    }
    finally {
      dbOptions.close();
    }
  }
};
