/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors All rights reserved.
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

package site.ycsb.measurements;

import java.util.ArrayList;
import java.util.List;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import site.ycsb.measurements.exporter.MeasurementsExporter;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Properties;
import java.util.Vector;

class SeriesUnit {
  /**
   * @param time
   * @param average
   */
  public SeriesUnit(long time, double average) {
    this.time = time;
    this.average = average;
  }

  protected final long time;
  protected final double average;
}

/**
 * A time series measurement of a metric, such as READ LATENCY.
 */
public class OneMeasurementTimeSeries extends OneMeasurement {

  /**
   * Granularity for time series; measurements will be averaged in chunks of this granularity. Units are milliseconds.
   */
  public static final String GRANULARITY = "timeseries.granularity";
  public static final String GRANULARITY_DEFAULT = "1000";

  public static final String PERCENTILES_PROPERTY = "timeseries.percentiles";
  public static final String PERCENTILES_PROPERTY_DEFAULT = "95,99";

  private final int granularity;
  private final Vector<SeriesUnit> measurements;
  private final Recorder histogram;
  private Histogram totalHistogram;
  private final List<Double> percentiles;

  private long start = -1;
  private long currentunit = -1;
  private long count = 0;
  private long sum = 0;
  private long operations = 0;
  private long totallatency = 0;

  //keep a windowed version of these stats for printing status
  private int windowoperations = 0;
  private long windowtotallatency = 0;

  private int min = -1;
  private int max = -1;

  public OneMeasurementTimeSeries(String name, Properties props) {
    super(name);
    granularity = Integer.parseInt(props.getProperty(GRANULARITY, GRANULARITY_DEFAULT));
    measurements = new Vector<>();
    histogram = new Recorder(3);
    percentiles = getPercentileValues(props.getProperty(PERCENTILES_PROPERTY, PERCENTILES_PROPERTY_DEFAULT));
  }

  private synchronized void checkEndOfUnit(boolean forceend) {
    long now = System.currentTimeMillis();

    if (start < 0) {
      currentunit = 0;
      start = now;
    }

    long unit = ((now - start) / granularity) * granularity;

    if ((unit > currentunit) || (forceend)) {
      double avg = ((double) sum) / ((double) count);
      measurements.add(new SeriesUnit(currentunit, avg));

      currentunit = unit;

      count = 0;
      sum = 0;
    }
  }

  @Override
  public synchronized void measure(int latency) {
    checkEndOfUnit(false);

    histogram.recordValue(latency);

    count++;
    sum += latency;
    totallatency += latency;
    operations++;
    windowoperations++;
    windowtotallatency += latency;

    if (latency > max) {
      max = latency;
    }

    if ((latency < min) || (min < 0)) {
      min = latency;
    }
  }

  @Override
  public void exportMeasurements(MeasurementsExporter exporter) throws IOException {
    checkEndOfUnit(true);

    // accumulate the last interval which was not caught by status thread
    Histogram intervalHistogram = getIntervalHistogramAndAccumulate();

    exporter.write(getName(), "Operations", operations);
    exporter.write(getName(), "AverageLatency(us)", (((double) totallatency) / ((double) operations)));
    exporter.write(getName(), "MinLatency(us)", min);
    exporter.write(getName(), "MaxLatency(us)", max);

    for (Double percentile : percentiles) {
      exporter.write(getName(), ordinal(percentile) + "PercentileLatency(us)",
          totalHistogram.getValueAtPercentile(percentile));
    }

    exportStatusCounts(exporter);
    for (SeriesUnit unit : measurements) {
      exporter.write(getName(), Long.toString(unit.time), unit.average);
    }
  }

  @Override
  public String getSummary() {
    if (windowoperations == 0) {
      return "";
    }
    DecimalFormat d = new DecimalFormat("#.##");
    double report = ((double) windowtotallatency) / ((double) windowoperations);
    windowtotallatency = 0;
    windowoperations = 0;
    return "[" + getName() + " AverageLatency(us)=" + d.format(report) + "]";
  }

  /**
   * Helper method to parse the given percentile value string.
   *
   * @param percentileString - comma delimited string of Integer values
   * @return An Integer List of percentile values
   */
  private List<Double> getPercentileValues(String percentileString) {
    List<Double> percentileValues = new ArrayList<>();

    try {
      for (String rawPercentile : percentileString.split(",")) {
        percentileValues.add(Double.parseDouble(rawPercentile));
      }
    } catch (Exception e) {
      // If the given timeseries.percentiles value is unreadable for whatever reason,
      // then calculate and return the default set.
      System.err.println("[WARN] Couldn't read " + PERCENTILES_PROPERTY + " value: '" + percentileString +
          "', the default of '" + PERCENTILES_PROPERTY_DEFAULT + "' will be used.");
      e.printStackTrace();
      return getPercentileValues(PERCENTILES_PROPERTY_DEFAULT);
    }

    return percentileValues;
  }

  /**
   * Helper method to find the ordinal of any number. eg 1 -> 1st
   * @param i number
   * @return ordinal string
   */
  private String ordinal(Double i) {
    String[] suffixes = new String[]{"th", "st", "nd", "rd", "th", "th", "th", "th", "th", "th"};
    Integer j = i.intValue();
    if (i % 1 == 0) {
      switch (j % 100) {
      case 11:
      case 12:
      case 13:
        return j + "th";
      default:
        return j + suffixes[j % 10];
      }
    } else {
      return i.toString();
    }
  }

  private Histogram getIntervalHistogramAndAccumulate() {
    Histogram intervalHistogram = histogram.getIntervalHistogram();
    // add this to the total time histogram.
    if (totalHistogram == null) {
      totalHistogram = intervalHistogram;
    } else {
      totalHistogram.add(intervalHistogram);
    }
    return intervalHistogram;
  }

}
