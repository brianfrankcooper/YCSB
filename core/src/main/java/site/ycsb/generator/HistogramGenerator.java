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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generate integers according to a histogram distribution.  The histogram
 * buckets are of width one, but the values are multiplied by a block size.
 * Therefore, instead of drawing sizes uniformly at random within each
 * bucket, we always draw the largest value in the current bucket, so the value
 * drawn is always a multiple of blockSize.
 *
 * The minimum value this distribution returns is blockSize (not zero).
 *
 */
public class HistogramGenerator extends NumberGenerator {

  private final long blockSize;
  private final long[] buckets;
  private long area;
  private long weightedArea = 0;
  private double meanSize = 0;

  public HistogramGenerator(String histogramfile) throws IOException {
    try (BufferedReader in = new BufferedReader(new FileReader(histogramfile))) {
      String str;
      String[] line;

      ArrayList<Integer> a = new ArrayList<>();

      str = in.readLine();
      if (str == null) {
        throw new IOException("Empty input file!\n");
      }
      line = str.split("\t");
      if (line[0].compareTo("BlockSize") != 0) {
        throw new IOException("First line of histogram is not the BlockSize!\n");
      }
      blockSize = Integer.parseInt(line[1]);

      while ((str = in.readLine()) != null) {
        // [0] is the bucket, [1] is the value
        line = str.split("\t");

        a.add(Integer.parseInt(line[0]), Integer.parseInt(line[1]));
      }
      buckets = new long[a.size()];
      for (int i = 0; i < a.size(); i++) {
        buckets[i] = a.get(i);
      }
    }
    init();
  }

  public HistogramGenerator(long[] buckets, int blockSize) {
    this.blockSize = blockSize;
    this.buckets = buckets;
    init();
  }

  private void init() {
    for (int i = 0; i < buckets.length; i++) {
      area += buckets[i];
      weightedArea += i * buckets[i];
    }
    // calculate average file size
    meanSize = ((double) blockSize) * ((double) weightedArea) / (area);
  }

  @Override
  public Long nextValue() {
    int number = ThreadLocalRandom.current().nextInt((int) area);
    int i;

    for (i = 0; i < (buckets.length - 1); i++) {
      number -= buckets[i];
      if (number <= 0) {
        return (i + 1) * blockSize;
      }
    }

    return i * blockSize;
  }

  @Override
  public double mean() {
    return meanSize;
  }
}
