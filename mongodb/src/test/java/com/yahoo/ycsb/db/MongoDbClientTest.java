/*
 * Copyright (c) 2014, Yahoo!, Inc. All rights reserved.
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
package com.yahoo.ycsb.db;

import static org.junit.Assume.assumeNoException;

import java.util.Properties;

import org.junit.After;

import com.yahoo.ycsb.DB;

/**
 * MongoDbClientTest provides runs the basic workload operations.
 */
public class MongoDbClientTest extends AbstractDBTestCases {

  /** The client to use. */
  private MongoDbClient myClient = null;

  /**
   * Stops the test client.
   */
  @After
  public void tearDown() {
    try {
      myClient.cleanup();
    } catch (Exception error) {
      // Ignore.
    } finally {
      myClient = null;
    }
  }

  /**
   * {@inheritDoc}
   * <p>
   * Overridden to return the {@link MongoDbClient}.
   * </p>
   */
  @Override
  protected DB getDB(Properties props) {
    if( myClient == null ) {
      myClient = new MongoDbClient();
      myClient.setProperties(props);
      try {
        myClient.init();
      } catch (Exception error) {
        assumeNoException(error);
      }
    }
    return myClient;
  }
}
