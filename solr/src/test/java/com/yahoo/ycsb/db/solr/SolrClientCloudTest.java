/**
 * Copyright (c) 2016 YCSB contributors. All rights reserved.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package com.yahoo.ycsb.db.solr;

import com.yahoo.ycsb.DB;
import org.junit.After;

import java.util.Properties;

import static org.junit.Assume.assumeNoException;

public class SolrClientCloudTest extends SolrClientBaseTest {

  private SolrClient instance;

  @After
  public void tearDown() throws Exception {
    try {
      if(instance != null) {
        instance.cleanup();
      }
    } finally {
      super.tearDown();
    }
  }

  @Override
  protected DB getDB(Properties props) {
    instance = new SolrClient();

    props.setProperty("solr.cloud", "true");
    props.setProperty("solr.zookeeper.hosts", miniSolrCloudCluster.getSolrClient().getZkHost());

    instance.setProperties(props);
    try {
      instance.init();
    } catch (Exception error) {
      assumeNoException(error);
    }
    return instance;
  }
}
