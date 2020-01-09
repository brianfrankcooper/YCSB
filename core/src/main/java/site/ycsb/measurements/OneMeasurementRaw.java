/**
 * Copyright (c) 2015-2017 YCSB contributors All rights reserved.
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

import site.ycsb.measurements.exporter.MeasurementsExporter;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.Properties;

/**
 * Record a series of measurements as raw data points without down sampling,
 * optionally write to an output file when configured.
 *
 */
public class OneMeasurementRaw extends OneMeasurement {
  /**
   * One raw data point, two fields: timestamp (ms) when the datapoint is
   * inserted, and the value.
   */
  class RawDataPoint {
    private final long timestamp;
    private final int value;

    public RawDataPoint(int value) {
      this.timestamp = System.currentTimeMillis();
      this.value = value;
    }

    public long timeStamp() {
      return timestamp;
    }

    public int value() {
      return value;
    }
  }

  class RawDataPointComparator implements Comparator<RawDataPoint> {
    @Override
    public int compare(RawDataPoint p1, RawDataPoint p2) {
      if (p1.value() < p2.value()) {
        return -1;
      } else if (p1.value() == p2.value()) {
        return 0;
      } else {
        return 1;
      }
    }
  }

  /**
   * Optionally, user can configure an output file to save the raw data points.
   * Default is none, raw results will be written to stdout.
   *
   */
  public static final String OUTPUT_FILE_PATH = "measurement.raw.output_file";
  public static final String OUTPUT_FILE_PATH_DEFAULT = "";

  /**
   * Optionally, user can request to not output summary stats. This is useful
   * if the user chains the raw measurement type behind the HdrHistogram type
   * which already outputs summary stats. But even in that case, the user may
   * still want this class to compute summary stats for them, especially if
   * they want accurate computation of percentiles (because percentils computed
   * by histogram classes are still approximations).
   */
  public static final String NO_SUMMARY_STATS = "measurement.raw.no_summary";
  public static final String NO_SUMMARY_STATS_DEFAULT = "false";

  private final PrintStream outputStream;

  private boolean noSummaryStats = false;

  private LinkedList<RawDataPoint> measurements;
  private long totalLatency = 0;

  // A window of stats to print summary for at the next getSummary() call.
  // It's supposed to be a one line summary, so we will just print count and
  // average.
  private int windowOperations = 0;
  private long windowTotalLatency = 0;

  public OneMeasurementRaw(String name, Properties props) {
    super(name);

    String outputFilePath = props.getProperty(OUTPUT_FILE_PATH, OUTPUT_FILE_PATH_DEFAULT);
    if (!outputFilePath.isEmpty()) {
      System.out.println("Raw data measurement: will output to result file: " +
          outputFilePath);

      try {
        outputStream = new PrintStream(
            new FileOutputStream(outputFilePath, true),
            true);
      } catch (FileNotFoundException e) {
        throw new RuntimeException("Failed to open raw data output file", e);
      }

    } else {
      System.out.println("Raw data measurement: will output to stdout.");
      outputStream = System.out;

    }

    noSummaryStats = Boolean.parseBoolean(props.getProperty(NO_SUMMARY_STATS,
        NO_SUMMARY_STATS_DEFAULT));

    measurements = new LinkedList<>();
  }

  @Override
  public synchronized void measure(int latency) {
    totalLatency += latency;
    windowTotalLatency += latency;
    windowOperations++;

    measurements.add(new RawDataPoint(latency));
  }

  @Override
  public void exportMeasurements(MeasurementsExporter exporter)
      throws IOException {
    // Output raw data points first then print out a summary of percentiles to
    // stdout.

    outputStream.println(getName() +
        " latency raw data: op, timestamp(ms), latency(us)");
    for (RawDataPoint point : measurements) {
      outputStream.println(
          String.format("%s,%d,%d", getName(), point.timeStamp(),
              point.value()));
    }
    if (outputStream != System.out) {
      outputStream.close();
    }

    int totalOps = measurements.size();
    exporter.write(getName(), "Total Operations", totalOps);
    if (totalOps > 0 && !noSummaryStats) {
      exporter.write(getName(),
          "Below is a summary of latency in microseconds:", -1);
      exporter.write(getName(), "Average",
          (double) totalLatency / (double) totalOps);

      Collections.sort(measurements, new RawDataPointComparator());

      exporter.write(getName(), "Min", measurements.get(0).value());
      exporter.write(
          getName(), "Max", measurements.get(totalOps - 1).value());
      exporter.write(
          getName(), "p1", measurements.get((int) (totalOps * 0.01)).value());
      exporter.write(
          getName(), "p5", measurements.get((int) (totalOps * 0.05)).value());
      exporter.write(
          getName(), "p50", measurements.get((int) (totalOps * 0.5)).value());
      exporter.write(
          getName(), "p90", measurements.get((int) (totalOps * 0.9)).value());
      exporter.write(
          getName(), "p95", measurements.get((int) (totalOps * 0.95)).value());
      exporter.write(
          getName(), "p99", measurements.get((int) (totalOps * 0.99)).value());
      exporter.write(getName(), "p99.9",
          measurements.get((int) (totalOps * 0.999)).value());
      exporter.write(getName(), "p99.99",
          measurements.get((int) (totalOps * 0.9999)).value());
    }

    exportStatusCounts(exporter);
  }

  @Override
  public synchronized String getSummary() {
    if (windowOperations == 0) {
      return "";
    }

    String toReturn = String.format("%s count: %d, average latency(us): %.2f",
        getName(), windowOperations,
        (double) windowTotalLatency / (double) windowOperations);

    windowTotalLatency = 0;
    windowOperations = 0;

    return toReturn;
  }
}
