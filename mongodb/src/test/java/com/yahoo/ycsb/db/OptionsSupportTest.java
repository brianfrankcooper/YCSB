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

import static com.yahoo.ycsb.db.OptionsSupport.updateUrl;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Properties;

import org.junit.Test;

/**
 * OptionsSupportTest provides tests for the OptionsSupport class.
 *
 * @author rjm
 */
public class OptionsSupportTest {

  /**
   * Test method for {@link OptionsSupport#updateUrl(String, Properties)} for
   * {@code mongodb.maxconnections}.
   */
  @Test
  public void testUpdateUrlMaxConnections() {
    assertThat(
        updateUrl("mongodb://locahost:27017/",
            props("mongodb.maxconnections", "1234")),
        is("mongodb://locahost:27017/?maxPoolSize=1234"));
    assertThat(
        updateUrl("mongodb://locahost:27017/?foo=bar",
            props("mongodb.maxconnections", "1234")),
        is("mongodb://locahost:27017/?foo=bar&maxPoolSize=1234"));
    assertThat(
        updateUrl("mongodb://locahost:27017/?maxPoolSize=1",
            props("mongodb.maxconnections", "1234")),
        is("mongodb://locahost:27017/?maxPoolSize=1"));
    assertThat(
        updateUrl("mongodb://locahost:27017/?foo=bar", props("foo", "1234")),
        is("mongodb://locahost:27017/?foo=bar"));
  }

  /**
   * Test method for {@link OptionsSupport#updateUrl(String, Properties)} for
   * {@code mongodb.threadsAllowedToBlockForConnectionMultiplier}.
   */
  @Test
  public void testUpdateUrlWaitQueueMultiple() {
    assertThat(
        updateUrl(
            "mongodb://locahost:27017/",
            props("mongodb.threadsAllowedToBlockForConnectionMultiplier",
                "1234")),
        is("mongodb://locahost:27017/?waitQueueMultiple=1234"));
    assertThat(
        updateUrl(
            "mongodb://locahost:27017/?foo=bar",
            props("mongodb.threadsAllowedToBlockForConnectionMultiplier",
                "1234")),
        is("mongodb://locahost:27017/?foo=bar&waitQueueMultiple=1234"));
    assertThat(
        updateUrl(
            "mongodb://locahost:27017/?waitQueueMultiple=1",
            props("mongodb.threadsAllowedToBlockForConnectionMultiplier",
                "1234")), is("mongodb://locahost:27017/?waitQueueMultiple=1"));
    assertThat(
        updateUrl("mongodb://locahost:27017/?foo=bar", props("foo", "1234")),
        is("mongodb://locahost:27017/?foo=bar"));
  }

  /**
   * Test method for {@link OptionsSupport#updateUrl(String, Properties)} for
   * {@code mongodb.threadsAllowedToBlockForConnectionMultiplier}.
   */
  @Test
  public void testUpdateUrlWriteConcern() {
    assertThat(
        updateUrl("mongodb://locahost:27017/",
            props("mongodb.writeConcern", "errors_ignored")),
        is("mongodb://locahost:27017/?w=0"));
    assertThat(
        updateUrl("mongodb://locahost:27017/?foo=bar",
            props("mongodb.writeConcern", "unacknowledged")),
        is("mongodb://locahost:27017/?foo=bar&w=0"));
    assertThat(
        updateUrl("mongodb://locahost:27017/?foo=bar",
            props("mongodb.writeConcern", "acknowledged")),
        is("mongodb://locahost:27017/?foo=bar&w=1"));
    assertThat(
        updateUrl("mongodb://locahost:27017/?foo=bar",
            props("mongodb.writeConcern", "journaled")),
        is("mongodb://locahost:27017/?foo=bar&journal=true&j=true"));
    assertThat(
        updateUrl("mongodb://locahost:27017/?foo=bar",
            props("mongodb.writeConcern", "replica_acknowledged")),
        is("mongodb://locahost:27017/?foo=bar&w=2"));
    assertThat(
        updateUrl("mongodb://locahost:27017/?foo=bar",
            props("mongodb.writeConcern", "majority")),
        is("mongodb://locahost:27017/?foo=bar&w=majority"));

    // w already exists.
    assertThat(
        updateUrl("mongodb://locahost:27017/?w=1",
            props("mongodb.writeConcern", "acknowledged")),
        is("mongodb://locahost:27017/?w=1"));

    // Unknown options
    assertThat(
        updateUrl("mongodb://locahost:27017/?foo=bar", props("foo", "1234")),
        is("mongodb://locahost:27017/?foo=bar"));
  }

  /**
   * Test method for {@link OptionsSupport#updateUrl(String, Properties)} for
   * {@code mongodb.threadsAllowedToBlockForConnectionMultiplier}.
   */
  @Test
  public void testUpdateUrlReadPreference() {
    assertThat(
        updateUrl("mongodb://locahost:27017/",
            props("mongodb.readPreference", "primary")),
        is("mongodb://locahost:27017/?readPreference=primary"));
    assertThat(
        updateUrl("mongodb://locahost:27017/?foo=bar",
            props("mongodb.readPreference", "primary_preferred")),
        is("mongodb://locahost:27017/?foo=bar&readPreference=primaryPreferred"));
    assertThat(
        updateUrl("mongodb://locahost:27017/?foo=bar",
            props("mongodb.readPreference", "secondary")),
        is("mongodb://locahost:27017/?foo=bar&readPreference=secondary"));
    assertThat(
        updateUrl("mongodb://locahost:27017/?foo=bar",
            props("mongodb.readPreference", "secondary_preferred")),
        is("mongodb://locahost:27017/?foo=bar&readPreference=secondaryPreferred"));
    assertThat(
        updateUrl("mongodb://locahost:27017/?foo=bar",
            props("mongodb.readPreference", "nearest")),
        is("mongodb://locahost:27017/?foo=bar&readPreference=nearest"));

    // readPreference already exists.
    assertThat(
        updateUrl("mongodb://locahost:27017/?readPreference=primary",
            props("mongodb.readPreference", "secondary")),
        is("mongodb://locahost:27017/?readPreference=primary"));

    // Unknown options
    assertThat(
        updateUrl("mongodb://locahost:27017/?foo=bar", props("foo", "1234")),
        is("mongodb://locahost:27017/?foo=bar"));
  }

  /**
   * Factory method for a {@link Properties} object.
   * 
   * @param key
   *          The key for the property to set.
   * @param value
   *          The value for the property to set.
   * @return The {@link Properties} with the property added.
   */
  private Properties props(String key, String value) {
    Properties props = new Properties();

    props.setProperty(key, value);

    return props;
  }

}
