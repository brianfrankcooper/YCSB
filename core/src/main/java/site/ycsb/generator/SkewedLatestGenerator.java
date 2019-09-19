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

package site.ycsb.generator;

/**
 * Generate a popularity distribution of items, skewed to favor recent items significantly more than older items.
 */
public class SkewedLatestGenerator extends NumberGenerator {
  private CounterGenerator basis;
  private final ZipfianGenerator zipfian;

  public SkewedLatestGenerator(CounterGenerator basis) {
    this.basis = basis;
    zipfian = new ZipfianGenerator(this.basis.lastValue());
    nextValue();
  }

  /**
   * Generate the next string in the distribution, skewed Zipfian favoring the items most recently returned by
   * the basis generator.
   */
  @Override
  public Long nextValue() {
    long max = basis.lastValue();
    long next = max - zipfian.nextLong(max);
    setLastValue(next);
    return next;
  }

  public static void main(String[] args) {
    SkewedLatestGenerator gen = new SkewedLatestGenerator(new CounterGenerator(1000));
    for (int i = 0; i < Integer.parseInt(args[0]); i++) {
      System.out.println(gen.nextString());
    }
  }

  @Override
  public double mean() {
    throw new UnsupportedOperationException("Can't compute mean of non-stationary distribution!");
  }
}
