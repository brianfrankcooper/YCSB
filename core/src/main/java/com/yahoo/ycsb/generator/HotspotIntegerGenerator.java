/**
 * Copyright (c) 2010 Yahoo! Inc. Copyright (c) 2017 YCSB contributors. All rights reserved.
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

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generate integers resembling a hotspot distribution where x% of operations
 * access y% of data items. The parameters specify the bounds for the numbers,
 * the percentage of the of the interval which comprises the hot set and
 * the percentage of operations that access the hot set. Numbers of the hot set are
 * always smaller than any number in the cold set. Elements from the hot set and
 * the cold set are chose using a uniform distribution.
 *
 */
public class HotspotIntegerGenerator extends NumberGenerator {

  private final long lowerBound;
  private final long upperBound;
  private final long hotInterval;
  private final long coldInterval;
  private final double hotsetFraction;
  private final double hotOpnFraction;

  /**
   * Create a generator for Hotspot distributions.
   *
   * @param lowerBound lower bound of the distribution.
   * @param upperBound upper bound of the distribution.
   * @param hotsetFraction percentage of data item
   * @param hotOpnFraction percentage of operations accessing the hot set.
   */
  public HotspotIntegerGenerator(long lowerBound, long upperBound,
                                 double hotsetFraction, double hotOpnFraction) {
    if (hotsetFraction < 0.0 || hotsetFraction > 1.0) {
      System.err.println("Hotset fraction out of range. Setting to 0.0");
      hotsetFraction = 0.0;
    }
    if (hotOpnFraction < 0.0 || hotOpnFraction > 1.0) {
      System.err.println("Hot operation fraction out of range. Setting to 0.0");
      hotOpnFraction = 0.0;
    }
    if (lowerBound > upperBound) {
      System.err.println("Upper bound of Hotspot generator smaller than the lower bound. " +
          "Swapping the values.");
      long temp = lowerBound;
      lowerBound = upperBound;
      upperBound = temp;
    }
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
    this.hotsetFraction = hotsetFraction;
    long interval = upperBound - lowerBound + 1;
    this.hotInterval = (int) (interval * hotsetFraction);
    this.coldInterval = interval - hotInterval;
    this.hotOpnFraction = hotOpnFraction;
  }

  @Override
  public Long nextValue() {
    long value = 0;
    Random random = ThreadLocalRandom.current();
    if (random.nextDouble() < hotOpnFraction) {
      // Choose a value from the hot set.
      value = lowerBound + Math.abs(random.nextLong()) % hotInterval;
    } else {
      // Choose a value from the cold set.
      value = lowerBound + hotInterval + Math.abs(random.nextLong()) % coldInterval;
    }
    setLastValue(value);
    return value;
  }

  /**
   * @return the lowerBound
   */
  public long getLowerBound() {
    return lowerBound;
  }

  /**
   * @return the upperBound
   */
  public long getUpperBound() {
    return upperBound;
  }

  /**
   * @return the hotsetFraction
   */
  public double getHotsetFraction() {
    return hotsetFraction;
  }

  /**
   * @return the hotOpnFraction
   */
  public double getHotOpnFraction() {
    return hotOpnFraction;
  }

  @Override
  public double mean() {
    return hotOpnFraction * (lowerBound + hotInterval / 2.0)
        + (1 - hotOpnFraction) * (lowerBound + hotInterval + coldInterval / 2.0);
  }
}
