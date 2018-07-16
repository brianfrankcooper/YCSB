
/**
 * Copyright (c) 2013-2018 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 * <p>
 */

package com.yahoo.ycsb.db.ignite;

import com.yahoo.ycsb.DB;
import org.apache.ignite.Ignite;
import org.junit.After;
import org.junit.AfterClass;

/**
 * Common test class.
 */
public class IgniteClientCommonTest {
  /** */
  protected static Ignite cluster;

  /** */
  protected DB client;

  /**
   *
   */
  @After
  public void tearDown() throws Exception {
    client.cleanup();
  }

  /**
   *
   */
  @AfterClass
  public static void afterClass() {
    cluster.close();

    try {
      Thread.sleep(1000);
    }
    catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
