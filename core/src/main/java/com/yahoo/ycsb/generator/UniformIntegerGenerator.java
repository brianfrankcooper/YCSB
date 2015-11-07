/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
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

import com.yahoo.ycsb.Utils;

/**
 * Generates integers randomly uniform from an interval.
 */
public class UniformIntegerGenerator extends IntegerGenerator {
  private int lb;
  private int ub;
  private int interval;

  /**
   * Creates a generator that will return integers uniformly randomly from the
   * interval [lb,ub] inclusive (that is, lb and ub are possible values).
   *
   * @param lb
   *          the lower bound (inclusive) of generated values
   * @param ub
   *          the upper bound (inclusive) of generated values
   */
  public UniformIntegerGenerator(final int lb, final int ub) {
    this.lb = lb;
    this.ub = ub;
    this.interval = (ub - lb) + 1;
  }

  @Override
  public double mean() {
    return ((lb + (long) ub)) / 2.0;
  }

  @Override
  public int nextInt() {
    final int ret = Utils.random().nextInt(interval) + lb;
    setLastInt(ret);

    return ret;
  }
}
