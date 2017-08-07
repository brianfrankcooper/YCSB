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

package com.yahoo.ycsb.measurements;

import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramIterationValue;
import org.HdrHistogram.HistogramLogWriter;
import org.HdrHistogram.Recorder;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Take measurements and maintain a HdrHistogram of a given metric, such as READ LATENCY.
 *
 */
public class OneMeasurementHdrHistogram extends OneMeasurement {

  // we need one log per measurement histogram
  private final PrintStream log;
  private final HistogramLogWriter histogramLogWriter;

  private final Recorder histogram;
  private Histogram totalHistogram;

  /**
   * The name of the property for deciding what percentile values to output.
   */
  public static final String PERCENTILES_PROPERTY = "hdrhistogram.percentiles";

  /**
   * The default value for the hdrhistogram.percentiles property.
   */
  public static final String PERCENTILES_PROPERTY_DEFAULT = "95,99";

  private final List<Double> percentiles;

  public OneMeasurementHdrHistogram(String name, Properties props) {
    super(name);
    percentiles = getPercentileValues(props.getProperty(PERCENTILES_PROPERTY, PERCENTILES_PROPERTY_DEFAULT));
    boolean shouldLog = Boolean.parseBoolean(props.getProperty("hdrhistogram.fileoutput", "false"));
    if (!shouldLog) {
      log = null;
      histogramLogWriter = null;
    } else {
      try {
        final String hdrOutputFilename = props.getProperty("hdrhistogram.output.path", "") + name + ".hdr";
        log = new PrintStream(new FileOutputStream(hdrOutputFilename), false);
      } catch (FileNotFoundException e) {
        throw new RuntimeException("Failed to open hdr histogram output file", e);
      }
      histogramLogWriter = new HistogramLogWriter(log);
      histogramLogWriter.outputComment("[Logging for: " + name + "]");
      histogramLogWriter.outputLogFormatVersion();
      long now = System.currentTimeMillis();
      histogramLogWriter.outputStartTime(now);
      histogramLogWriter.setBaseTime(now);
      histogramLogWriter.outputLegend();
    }
    histogram = new Recorder(3);
  }

  /**
   * It appears latency is reported in micros.
   * Using {@link Recorder} to support concurrent updates to histogram.
   */
  public void measure(int latencyInMicros) {
    histogram.recordValue(latencyInMicros);
  }

  /**
   * This is called from a main thread, on orderly termination.
   */
  @Override
  public void exportMeasurements(MeasurementsExporter exporter) throws IOException {
    // accumulate the last interval which was not caught by status thread
    Histogram intervalHistogram = getIntervalHistogramAndAccumulate();
    if (histogramLogWriter != null) {
      histogramLogWriter.outputIntervalHistogram(intervalHistogram);
      // we can close now
      log.close();
    }
    exporter.write(getName(), "Operations", totalHistogram.getTotalCount());
    exporter.write(getName(), "AverageLatency(us)", totalHistogram.getMean());
    exporter.write(getName(), "MinLatency(us)", totalHistogram.getMinValue());
    exporter.write(getName(), "MaxLatency(us)", totalHistogram.getMaxValue());

    for (Double percentile : percentiles) {
      exporter.write(getName(), ordinal(percentile) + "PercentileLatency(us)",
          totalHistogram.getValueAtPercentile(percentile));
    }

    exportStatusCounts(exporter);

    // also export totalHistogram
    for (HistogramIterationValue v : totalHistogram.recordedValues()) {
      int value;
      if (v.getValueIteratedTo() > (long)Integer.MAX_VALUE) {
        value = Integer.MAX_VALUE;
      } else {
        value = (int)v.getValueIteratedTo();
      }

      exporter.write(getName(), Integer.toString(value), (double)v.getCountAtValueIteratedTo());
    }
  }

  /**
   * This is called periodically from the StatusThread. There's a single
   * StatusThread per Client process. We optionally serialize the interval to
   * log on this opportunity.
   *
   * @see com.yahoo.ycsb.measurements.OneMeasurement#getSummary()
   */
  @Override
  public String getSummary() {
    Histogram intervalHistogram = getIntervalHistogramAndAccumulate();
    // we use the summary interval as the histogram file interval.
    if (histogramLogWriter != null) {
      histogramLogWriter.outputIntervalHistogram(intervalHistogram);
    }

    DecimalFormat d = new DecimalFormat("#.##");
    return "[" + getName() + ": Count=" + intervalHistogram.getTotalCount() + ", Max="
        + intervalHistogram.getMaxValue() + ", Min=" + intervalHistogram.getMinValue() + ", Avg="
        + d.format(intervalHistogram.getMean()) + ", 90=" + d.format(intervalHistogram.getValueAtPercentile(90))
        + ", 99=" + d.format(intervalHistogram.getValueAtPercentile(99)) + ", 99.9="
        + d.format(intervalHistogram.getValueAtPercentile(99.9)) + ", 99.99="
        + d.format(intervalHistogram.getValueAtPercentile(99.99)) + "]";
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
      // If the given hdrhistogram.percentiles value is unreadable for whatever reason,
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
}
