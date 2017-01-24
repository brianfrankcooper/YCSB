/**
 * Copyright (c) 2015-2017 YCSB contributors. All rights reserved.
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
package com.yahoo.ycsb.generator;

import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.testng.annotations.Test;

/**
 * Tests for the AcknowledgedCounterGenerator class.
 */
public class AcknowledgedCounterGeneratorTest {

  /**
   * Test that advancing past {@link Integer#MAX_VALUE} works.
   */
  @Test
  public void testIncrementPastIntegerMaxValue() {
    final long toTry = AcknowledgedCounterGenerator.WINDOW_SIZE * 3;

    AcknowledgedCounterGenerator generator =
        new AcknowledgedCounterGenerator(Integer.MAX_VALUE - 1000);

    Random rand = new Random(System.currentTimeMillis());
    BlockingQueue<Long> pending = new ArrayBlockingQueue<Long>(1000);
    for (long i = 0; i < toTry; ++i) {
      long value = generator.nextValue();

      while (!pending.offer(value)) {

        Long first = pending.poll();

        // Don't always advance by one.
        if (rand.nextBoolean()) {
          generator.acknowledge(first);
        } else {
          Long second = pending.poll();
          pending.add(first);
          generator.acknowledge(second);
        }
      }
    }

  }
}
