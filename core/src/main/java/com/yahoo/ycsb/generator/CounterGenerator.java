/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors. All rights reserved.
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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Generates a sequence of integers.
 * (0, 1, ...)
 */
public class CounterGenerator extends NumberGenerator {
  private final AtomicInteger counter;

  /**
   * Create a counter that starts at countstart.
   */
  public CounterGenerator(int countstart) {
    counter = new AtomicInteger(countstart);
  }

  @Override
  public Integer nextValue() {
    return counter.getAndIncrement();
  }

  @Override
  public Integer lastValue() {
    return counter.get() - 1;
  }

  @Override
  public double mean() {
    throw new UnsupportedOperationException("Can't compute mean of non-stationary distribution!");
  }
}
