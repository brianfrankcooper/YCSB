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

import com.yahoo.ycsb.Utils;

/**
 * Generates integers randomly uniform from an interval.
 */
public class UniformIntegerGenerator extends NumberGenerator {
  private final int lb, ub, interval;

  /**
   * Creates a generator that will return integers uniformly randomly from the interval [lb,ub] inclusive.
   * (lb and ub are possible values).
   *
   * @param lb the lower bound (inclusive) of generated values
   * @param ub the upper bound (inclusive) of generated values
   */
  public UniformIntegerGenerator(int lb, int ub) {
    this.lb = lb;
    this.ub = ub;
    interval = this.ub - this.lb + 1;
  }

  @Override
  public Integer nextValue() {
    int ret = Utils.random().nextInt(interval) + lb;
    setLastValue(ret);

    return ret;
  }

  @Override
  public double mean() {
    return ((lb + (long) ub)) / 2.0;
  }
}
