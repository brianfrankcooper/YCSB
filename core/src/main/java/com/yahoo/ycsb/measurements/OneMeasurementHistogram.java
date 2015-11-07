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

package com.yahoo.ycsb.measurements;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Properties;

import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;

/**
 * Take measurements and maintain a histogram of a given metric, such as READ
 * LATENCY.
 *
 * @author cooperb
 *
 */
public class OneMeasurementHistogram extends OneMeasurement {
  public static final String BUCKETS = "histogram.buckets";
  public static final String BUCKETS_DEFAULT = "1000";

  private int buckets;
  private int[] histogram;
  private int histogramoverflow;
  private int max;
  private int min;

  private int operations;
  private long totallatency;

  // keep a windowed version of these stats for printing status
  private int windowoperations;
  private long windowtotallatency;

  public OneMeasurementHistogram(final String name, final Properties props) {
    super(name);
    buckets = Integer.parseInt(props.getProperty(BUCKETS, BUCKETS_DEFAULT));
    histogram = new int[buckets];
    histogramoverflow = 0;
    operations = 0;
    totallatency = 0;
    windowoperations = 0;
    windowtotallatency = 0;
    min = -1;
    max = -1;
  }

  @Override
  public void exportMeasurements(final MeasurementsExporter exporter)
      throws IOException {
    exporter.write(getName(), "Operations", operations);
    exporter.write(getName(), "AverageLatency(us)",
        (((double) totallatency) / ((double) operations)));
    exporter.write(getName(), "MinLatency(us)", min);
    exporter.write(getName(), "MaxLatency(us)", max);

    int opcounter = 0;
    boolean done95th = false;
    for (int i = 0; i < buckets; i++) {
      opcounter += histogram[i];
      if ((!done95th)
          && ((((double) opcounter) / ((double) operations)) >= 0.95)) {
        exporter.write(getName(), "95thPercentileLatency(us)", i * 1000);
        done95th = true;
      }
      if ((((double) opcounter) / ((double) operations)) >= 0.99) {
        exporter.write(getName(), "99thPercentileLatency(us)", i * 1000);
        break;
      }
    }

    exportStatusCounts(exporter);

    for (int i = 0; i < buckets; i++) {
      exporter.write(getName(), Integer.toString(i), histogram[i]);
    }
    exporter.write(getName(), ">" + buckets, histogramoverflow);
  }

  @Override
  public String getSummary() {
    if (windowoperations == 0) {
      return "";
    }
    final DecimalFormat d = new DecimalFormat("#.##");
    final double report =
        ((double) windowtotallatency) / ((double) windowoperations);
    windowtotallatency = 0;
    windowoperations = 0;
    return "[" + getName() + " AverageLatency(us)=" + d.format(report) + "]";
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.yahoo.ycsb.OneMeasurement#measure(int)
   */
  @Override
  public synchronized void measure(final int latency) {
    if ((latency / 1000) >= buckets) {
      histogramoverflow++;
    } else {
      histogram[latency / 1000]++;
    }
    operations++;
    totallatency += latency;
    windowoperations++;
    windowtotallatency += latency;

    if ((min < 0) || (latency < min)) {
      min = latency;
    }

    if ((max < 0) || (latency > max)) {
      max = latency;
    }
  }
}
