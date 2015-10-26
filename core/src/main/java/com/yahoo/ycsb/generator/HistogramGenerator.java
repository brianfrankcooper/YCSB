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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import com.yahoo.ycsb.Utils;

/**
 * Generate integers according to a histogram distribution. The histogram
 * buckets are of width one, but the values are multiplied by a block size.
 * Therefore, instead of drawing sizes uniformly at random within each bucket,
 * we always draw the largest value in the current bucket, so the value drawn is
 * always a multiple of block_size.
 *
 * The minimum value this distribution returns is block_size (not zero).
 *
 * Modified Nov 19 2010 by sears
 *
 * @author snjones
 *
 */
public class HistogramGenerator extends IntegerGenerator {

  private long area;
  private final long blockSize;
  private final long[] buckets;
  private double meanSize = 0;
  private long weightedArea = 0;

  public HistogramGenerator(final long[] buckets, final int blockSize) {
    this.blockSize = blockSize;
    this.buckets = buckets;
    init();
  }

  public HistogramGenerator(final String histogramfile) throws IOException {
    final BufferedReader in = new BufferedReader(new FileReader(histogramfile));
    String str;
    String[] line;

    final ArrayList<Integer> a = new ArrayList<Integer>();

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

    in.close();
    init();
  }

  @Override
  public double mean() {
    return meanSize;
  }

  @Override
  public int nextInt() {
    int number = Utils.random().nextInt((int) area);
    int i;

    for (i = 0; i < (buckets.length - 1); i++) {
      number -= buckets[i];
      if (number <= 0) {
        return (int) ((i + 1) * blockSize);
      }
    }

    return (int) (i * blockSize);
  }

  private void init() {
    for (int i = 0; i < buckets.length; i++) {
      area += buckets[i];
      weightedArea = i * buckets[i];
    }
    // calculate average file size
    meanSize = (((double) blockSize) * ((double) weightedArea)) / (area);
  }
}
