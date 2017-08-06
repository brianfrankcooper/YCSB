/**
 * Copyright (c) 2017 YCSB contributors. All rights reserved.
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
package com.yahoo.ycsb.generator;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.testng.annotations.Test;
import org.testng.collections.Lists;

public class TestRandomDiscreteTimestampGenerator {

  @Test
  public void systemTime() throws Exception {
    final RandomDiscreteTimestampGenerator generator = 
        new RandomDiscreteTimestampGenerator(60, TimeUnit.SECONDS, 60);
    List<Long> generated = Lists.newArrayList();
    for (int i = 0; i < 60; i++) {
      generated.add(generator.nextValue());
    }
    assertEquals(generated.size(), 60);
    try {
      generator.nextValue();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
  }
  
  @Test
  public void withStartTime() throws Exception {
    final RandomDiscreteTimestampGenerator generator = 
        new RandomDiscreteTimestampGenerator(60, TimeUnit.SECONDS, 1072915200L, 60);
    List<Long> generated = Lists.newArrayList();
    for (int i = 0; i < 60; i++) {
      generated.add(generator.nextValue());
    }
    assertEquals(generated.size(), 60);
    Collections.sort(generated);
    long ts = 1072915200L - 60; // starts 1 interval in the past
    for (final long t : generated) {
      assertEquals(t, ts);
      ts += 60;
    }
    try {
      generator.nextValue();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
  }
  
  @Test (expectedExceptions = IllegalArgumentException.class)
  public void tooLarge() throws Exception {
    new RandomDiscreteTimestampGenerator(60, TimeUnit.SECONDS, 
        RandomDiscreteTimestampGenerator.MAX_INTERVALS + 1);
  }
  
  //TODO - With PowerMockito we could UT the initializeTimestamp(long) call.
  // Otherwise it would involve creating more functions and that would get ugly.
}
