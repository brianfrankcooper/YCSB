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
package site.ycsb.generator;

import java.util.concurrent.TimeUnit;

import site.ycsb.Utils;

/**
 * A generator that picks from a discrete set of offsets from a base Unix Epoch
 * timestamp that returns timestamps in a random order with the guarantee that
 * each timestamp is only returned once.
 * <p>
 * TODO - It would be best to implement some kind of psuedo non-repeating random
 * generator for this as it's likely OK that some small percentage of values are
 * repeated. For now we just generate all of the offsets in an array, shuffle
 * it and then iterate over the array.
 * <p>
 * Note that {@link #MAX_INTERVALS} defines a hard limit on the size of the 
 * offset array so that we don't completely blow out the heap.
 * <p>
 * The constructor parameter {@code intervals} determines how many values will be
 * returned by the generator. For example, if the {@code interval} is 60 and the
 * {@code timeUnits} are set to {@link TimeUnit#SECONDS} and {@code intervals} 
 * is set to 60, then the consumer can call {@link #nextValue()} 60 times for
 * timestamps within an hour.
 */
public class RandomDiscreteTimestampGenerator extends UnixEpochTimestampGenerator {

  /** A hard limit on the size of the offsets array to a void using too much heap. */
  public static final int MAX_INTERVALS = 16777216;
  
  /** The total number of intervals for this generator. */
  private final int intervals;
  
  // can't be primitives due to the generic params on the sort function :(
  /** The array of generated offsets from the base time. */
  private final Integer[] offsets;
  
  /** The current index into the offsets array. */
  private int offsetIndex;
  
  /**
   * Ctor that uses the current system time as current.
   * @param interval The interval between timestamps.
   * @param timeUnits The time units of the returned Unix Epoch timestamp (as well
   * as the units for the interval).
   * @param intervals The total number of intervals for the generator.
   * @throws IllegalArgumentException if the intervals is larger than {@link #MAX_INTERVALS}
   */
  public RandomDiscreteTimestampGenerator(final long interval, final TimeUnit timeUnits, 
                                          final int intervals) {
    super(interval, timeUnits);
    this.intervals = intervals;
    offsets = new Integer[intervals];
    setup();
  }
  
  /**
   * Ctor for supplying a starting timestamp.
   * The interval between timestamps.
   * @param timeUnits The time units of the returned Unix Epoch timestamp (as well
   * as the units for the interval).
   * @param startTimestamp The start timestamp to use. 
   * NOTE that this must match the time units used for the interval. 
   * If the units are in nanoseconds, provide a nanosecond timestamp {@code System.nanoTime()}
   * or in microseconds, {@code System.nanoTime() / 1000}
   * or in millis, {@code System.currentTimeMillis()}
   * @param intervals The total number of intervals for the generator.
   * @throws IllegalArgumentException if the intervals is larger than {@link #MAX_INTERVALS}
   */
  public RandomDiscreteTimestampGenerator(final long interval, final TimeUnit timeUnits,
                                          final long startTimestamp, final int intervals) {
    super(interval, timeUnits, startTimestamp);
    this.intervals = intervals;
    offsets = new Integer[intervals];
    setup();
  }
  
  /**
   * Generates the offsets and shuffles the array.
   */
  private void setup() {
    if (intervals > MAX_INTERVALS) {
      throw new IllegalArgumentException("Too many intervals for the in-memory "
          + "array. The limit is " + MAX_INTERVALS + ".");
    }
    offsetIndex = 0;
    for (int i = 0; i < intervals; i++) {
      offsets[i] = i;
    }
    Utils.shuffleArray(offsets);
  }
  
  @Override
  public Long nextValue() {
    if (offsetIndex >= offsets.length) {
      throw new IllegalStateException("Reached the end of the random timestamp "
          + "intervals: " + offsetIndex);
    }
    lastTimestamp = currentTimestamp;
    currentTimestamp = startTimestamp + (offsets[offsetIndex++] * getOffset(1));
    return currentTimestamp;
  }
}