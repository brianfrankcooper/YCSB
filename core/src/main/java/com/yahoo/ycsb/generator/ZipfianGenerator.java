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
 * A generator of a zipfian distribution. It produces a sequence of items, such
 * that some items are more popular than others, according to a zipfian
 * distribution. When you construct an instance of this class, you specify the
 * number of items in the set to draw from, either by specifying an itemcount
 * (so that the sequence is of items from 0 to itemcount-1) or by specifying a
 * min and a max (so that the sequence is of items from min to max inclusive).
 * After you construct the instance, you can change the number of items by
 * calling nextInt(itemcount) or nextLong(itemcount).
 *
 * Note that the popular items will be clustered together, e.g. item 0 is the
 * most popular, item 1 the second most popular, and so on (or min is the most
 * popular, min+1 the next most popular, etc.) If you don't want this
 * clustering, and instead want the popular items scattered throughout the item
 * space, then use ScrambledZipfianGenerator instead.
 *
 * Be aware: initializing this generator may take a long time if there are lots
 * of items to choose from (e.g. over a minute for 100 million objects). This is
 * because certain mathematical values need to be computed to properly generate
 * a zipfian skew, and one of those values (zeta) is a sum sequence from 1 to n,
 * where n is the itemcount. Note that if you increase the number of items in
 * the set, we can compute a new zeta incrementally, so it should be fast unless
 * you have added millions of items. However, if you decrease the number of
 * items, we recompute zeta from scratch, so this can take a long time.
 *
 * The algorithm used here is from
 * "Quickly Generating Billion-Record Synthetic Databases", Jim Gray et al,
 * SIGMOD 1994.
 */
public class ZipfianGenerator extends IntegerGenerator {
  public static final double ZIPFIAN_CONSTANT = 0.99;

  public static void main(final String[] args) {
    new ZipfianGenerator(ScrambledZipfianGenerator.ITEM_COUNT);
  }

  /**
   * Compute the zeta constant needed for the distribution. Do this from scratch
   * for a distribution with n items, using the zipfian constant theta. This is
   * a static version of the function which will not remember n.
   * 
   * @param n
   *          The number of items to compute zeta over.
   * @param theta
   *          The zipfian constant.
   */
  static double zetastatic(final long n, final double theta) {
    return zetastatic(0, n, theta, 0);
  }

  /**
   * Compute the zeta constant needed for the distribution. Do this
   * incrementally for a distribution that has n items now but used to have st
   * items. Use the zipfian constant theta. Remember the new value of n so that
   * if we change the itemcount, we'll know to recompute zeta.
   * 
   * @param st
   *          The number of items used to compute the last initialsum
   * @param n
   *          The number of items to compute zeta over.
   * @param theta
   *          The zipfian constant.
   * @param initialsum
   *          The value of zeta we are computing incrementally from.
   */
  static double zetastatic(final long st, final long n, final double theta,
      final double initialsum) {
    double sum = initialsum;
    for (long i = st; i < n; i++) {

      sum += 1 / (Math.pow(i + 1, theta));
    }

    // System.out.println("countforzeta="+countforzeta);

    return sum;
  }

  /**
   * Flag to prevent problems. If you increase the number of items the zipfian
   * generator is allowed to choose from, this code will incrementally compute a
   * new zeta value for the larger itemcount. However, if you decrease the
   * number of items, the code computes zeta from scratch; this is expensive for
   * large itemsets. Usually this is not intentional; e.g. one thread thinks the
   * number of items is 1001 and calls "nextLong()" with that item count; then
   * another thread who thinks the number of items is 1000 calls nextLong() with
   * itemcount=1000 triggering the expensive recomputation. (It is expensive for
   * 100 million items, not really for 1000 items.) Why did the second thread
   * think there were only 1000 items? maybe it read the item count before the
   * first thread incremented it. So this flag allows you to say if you really
   * do want that recomputation. If true, then the code will recompute zeta if
   * the itemcount goes down. If false, the code will assume itemcount only goes
   * up, and never recompute.
   */
  private boolean allowitemcountdecrease = false;

  /**
   * Computed parameters for generating the distribution.
   */
  private double alpha;
  private double zetan;
  private double eta;
  private double theta;
  private double zeta2theta;

  /**
   * Min item to generate.
   */
  private long base;

  /**
   * The number of items used to compute zetan the last time.
   */
  private long countforzeta;

  /**
   * Number of items.
   */
  private long items;

  /**
   * The zipfian constant to use.
   */
  private double zipfianconstant;

  /**
   * Create a zipfian generator for the specified number of items.
   * 
   * @param items
   *          The number of items in the distribution.
   */
  public ZipfianGenerator(final long items) {
    this(0, items - 1);
  }

  /**
   * Create a zipfian generator for the specified number of items using the
   * specified zipfian constant.
   * 
   * @param items
   *          The number of items in the distribution.
   * @param zipfianconstant
   *          The zipfian constant to use.
   */
  public ZipfianGenerator(final long items, final double zipfianconstant) {
    this(0, items - 1, zipfianconstant);
  }

  /**
   * Create a zipfian generator for items between min and max.
   * 
   * @param min
   *          The smallest integer to generate in the sequence.
   * @param max
   *          The largest integer to generate in the sequence.
   */
  public ZipfianGenerator(final long min, final long max) {
    this(min, max, ZIPFIAN_CONSTANT);
  }

  /**
   * Create a zipfian generator for items between min and max (inclusive) for
   * the specified zipfian constant.
   * 
   * @param min
   *          The smallest integer to generate in the sequence.
   * @param max
   *          The largest integer to generate in the sequence.
   * @param zipfianconstant
   *          The zipfian constant to use.
   */
  public ZipfianGenerator(final long min, final long max,
      final double zipfianconstant) {
    this(min, max, zipfianconstant,
        zetastatic((max - min) + 1, zipfianconstant));
  }

  /**
   * Create a zipfian generator for items between min and max (inclusive) for
   * the specified zipfian constant, using the precomputed value of zeta.
   * 
   * @param min
   *          The smallest integer to generate in the sequence.
   * @param max
   *          The largest integer to generate in the sequence.
   * @param zipfianconstant
   *          The zipfian constant to use.
   * @param zetan
   *          The precomputed zeta constant.
   */
  public ZipfianGenerator(final long min, final long max,
      final double zipfianconstant, final double zetan) {

    this.items = (max - min) + 1;
    this.base = min;
    this.zipfianconstant = zipfianconstant;

    this.theta = zipfianconstant;

    this.zeta2theta = zeta(2, theta);

    this.alpha = 1.0 / (1.0 - theta);
    // zetan=zeta(items,theta);
    this.zetan = zetan;
    this.countforzeta = items;
    this.eta =
        (1 - Math.pow(2.0 / items, 1 - theta)) / (1 - (zeta2theta / zetan));

    // System.out.println("XXXX 3 XXXX");
    nextInt();
    // System.out.println("XXXX 4 XXXX");
  }

  /**
   * @todo Implement ZipfianGenerator.mean()
   */
  @Override
  public double mean() {
    throw new UnsupportedOperationException(
        "@todo implement ZipfianGenerator.mean()");
  }

  /**
   * Return the next value, skewed by the Zipfian distribution. The 0th item
   * will be the most popular, followed by the 1st, followed by the 2nd, etc.
   * (Or, if min != 0, the min-th item is the most popular, the min+1th item the
   * next most popular, etc.) If you want the popular items scattered throughout
   * the item space, use ScrambledZipfianGenerator instead.
   */
  @Override
  public int nextInt() {
    return (int) nextLong(items);
  }

  /**
   * Generate the next item. this distribution will be skewed toward lower
   * integers; e.g. 0 will be the most popular, 1 the next most popular, etc.
   * 
   * @param itemcount
   *          The number of items in the distribution.
   * @return The next item in the sequence.
   */
  public int nextInt(final int itemcount) {
    return (int) nextLong(itemcount);
  }

  /**
   * Return the next value, skewed by the Zipfian distribution. The 0th item
   * will be the most popular, followed by the 1st, followed by the 2nd, etc.
   * (Or, if min != 0, the min-th item is the most popular, the min+1th item the
   * next most popular, etc.) If you want the popular items scattered throughout
   * the item space, use ScrambledZipfianGenerator instead.
   */
  public long nextLong() {
    return nextLong(items);
  }

  /**
   * Generate the next item as a long.
   * 
   * @param itemcount
   *          The number of items in the distribution.
   * @return The next item in the sequence.
   */
  public long nextLong(final long itemcount) {
    // from "Quickly Generating Billion-Record Synthetic Databases", Jim Gray et
    // al, SIGMOD 1994

    if (itemcount != countforzeta) {

      // have to recompute zetan and eta, since they depend on itemcount
      synchronized (this) {
        if (itemcount > countforzeta) {
          // System.err.println("WARNING: Incrementally recomputing Zipfian
          // distribtion. (itemcount="+itemcount+"
          // countforzeta="+countforzeta+")");

          // we have added more items. can compute zetan incrementally, which is
          // cheaper
          zetan = zeta(countforzeta, itemcount, theta, zetan);
          eta = (1 - Math.pow(2.0 / items, 1 - theta))
              / (1 - (zeta2theta / zetan));
        } else if ((itemcount < countforzeta) && (allowitemcountdecrease)) {
          // have to start over with zetan
          // note : for large itemsets, this is very slow. so don't do it!

          // Note: can also have a negative incremental computation, e.g. if you
          // decrease the number of items, then just subtract
          // the zeta sequence terms for the items that went away. This would be
          // faster than recomputing from scratch when the number of items
          // decreases

          System.err.println(
              "WARNING: Recomputing Zipfian distribtion. This is slow and "
                  + "should be avoided. (itemcount=" + itemcount
                  + " countforzeta=" + countforzeta + ")");

          zetan = zeta(itemcount, theta);
          eta = (1 - Math.pow(2.0 / items, 1 - theta))
              / (1 - (zeta2theta / zetan));
        }
      }
    }

    final double u = Utils.random().nextDouble();
    final double uz = u * zetan;

    if (uz < 1.0) {
      return base;
    }

    if (uz < (1.0 + Math.pow(0.5, theta))) {
      return base + 1;
    }

    final long ret =
        base + (long) ((itemcount) * Math.pow(((eta * u) - eta) + 1, alpha));
    setLastInt((int) ret);
    return ret;
  }

  /**
   * Compute the zeta constant needed for the distribution. Do this from scratch
   * for a distribution with n items, using the zipfian constant theta. Remember
   * the value of n, so if we change the itemcount, we can recompute zeta.
   * 
   * @param n
   *          The number of items to compute zeta over.
   * @param newTheta
   *          The zipfian constant.
   */
  double zeta(final long n, final double newTheta) {
    countforzeta = n;
    return zetastatic(n, newTheta);
  }

  /**
   * Compute the zeta constant needed for the distribution. Do this
   * incrementally for a distribution that has n items now but used to have st
   * items. Use the zipfian constant theta. Remember the new value of n so that
   * if we change the itemcount, we'll know to recompute zeta.
   * 
   * @param st
   *          The number of items used to compute the last initialsum
   * @param n
   *          The number of items to compute zeta over.
   * @param newTheta
   *          The zipfian constant.
   * @param initialsum
   *          The value of zeta we are computing incrementally from.
   */
  double zeta(final long st, final long n, final double newTheta,
      final double initialsum) {
    countforzeta = n;
    return zetastatic(st, n, newTheta, initialsum);
  }
}
