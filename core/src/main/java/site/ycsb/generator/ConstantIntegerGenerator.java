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
 * A trivial integer generator that always returns the same value.
 *
 */
public class ConstantIntegerGenerator extends NumberGenerator {
  private final int i;

  /**
   * @param i The integer that this generator will always return.
   */
  public ConstantIntegerGenerator(int i) {
    this.i = i;
  }

  @Override
  public Integer nextValue() {
    return i;
  }

  @Override
  public double mean() {
    return i;
  }

}
