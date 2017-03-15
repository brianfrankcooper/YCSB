/**
 * Copyright (c) 2016-2017 YCSB Contributors All rights reserved.
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
 * Generates a sequence of integers 0, 1, ...
 */
public class SequentialGenerator extends NumberGenerator {
  protected final AtomicInteger counter;
  protected int interval, countstart;

  /**
   * Create a counter that starts at countstart.
   */
  public SequentialGenerator(int countstart, int countend) {
    counter = new AtomicInteger();
    setLastValue(counter.get());
    this.countstart = countstart;
    interval = countend - countstart + 1;
  }

  /**
   * If the generator returns numeric (integer) values, return the next value as an int.
   * Default is to return -1, which is appropriate for generators that do not return numeric values.
   */
  public int nextInt() {
    int ret = countstart + counter.getAndIncrement() % interval;
    setLastValue(ret);
    return ret;
  }

  @Override
  public Number nextValue() {
    int ret = countstart + counter.getAndIncrement() % interval;
    setLastValue(ret);
    return ret;
  }

  @Override
  public Number lastValue() {
    return counter.get() + 1;
  }

  @Override
  public double mean() {
    throw new UnsupportedOperationException("Can't compute mean of non-stationary distribution!");
  }
}
