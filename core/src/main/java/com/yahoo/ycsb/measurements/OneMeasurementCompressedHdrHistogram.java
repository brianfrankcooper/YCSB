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

import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;
import org.HdrHistogram.Recorder;

import java.io.*;
import java.text.DecimalFormat;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Take measurements and maintain a HdrHistogram of a given metric, such as
 * READ LATENCY.
 *
 * @author nitsanw
 *
 */
public class OneMeasurementCompressedHdrHistogram extends OneMeasurement {

  // we need one log per measurement histogram
  PrintStream log;
  HistogramLogWriter histogramLogWriter;
  ByteArrayOutputStream compressedHistogram = new ByteArrayOutputStream();
  final Recorder histogram;
  Histogram totalHistogram;

  public OneMeasurementCompressedHdrHistogram(String name, Properties props) {
    super(name);
    boolean shouldLog = Boolean.parseBoolean(props.getProperty("hdrhistogram.fileoutput", "false"));
    if (!shouldLog) {
      log = null;
      histogramLogWriter = null;
    } else {
      try {
        log = new PrintStream(compressedHistogram, false, "UTF-8");

        histogramLogWriter = new HistogramLogWriter(log);
        //histogramLogWriter.outputComment("[Logging for: " + name + "]");
        //histogramLogWriter.outputLogFormatVersion();
        long now = System.currentTimeMillis();
        //histogramLogWriter.outputStartTime(now);
        histogramLogWriter.setBaseTime(now);
        //histogramLogWriter.outputLegend();
      } catch (UnsupportedEncodingException e) {
        e.printStackTrace();
      }
    }
    histogram = new Recorder(3);
  }

  /**
    * It appears latency is reported in micros.
    * Using {@link Recorder} to support concurrent updates to histogram.
    *
    * @see com.yahoo.ycsb.measurements.OneMeasurement#measure(int)
    */
  public void measure(int latencyInMicros) {
    histogram.recordValue(latencyInMicros);
  }

  /**
    * This is called from a main thread, on orderly termination.
    *
    * @see OneMeasurement#exportMeasurements(MeasurementsExporter)
    */
  @Override
  public void exportMeasurements(MeasurementsExporter exporter) throws IOException {
    // accumulate the last interval which was not caught by status thread
    Histogram intervalHistogram = getIntervalHistogramAndAccumulate();
    if(histogramLogWriter != null) {
      histogramLogWriter.outputIntervalHistogram(intervalHistogram);
      // we can close now
      log.close();
    }

    exporter.write(getName(), compressedHistogram.toString("UTF8") , 0);

    exportReturnCodes(exporter);
  }

  /**
    * This is called periodically from the StatusThread. There's a single StatusThread per Client process.
    * We optionally serialize the interval to log on this opportunity.
    * @see OneMeasurement#getSummary()
    */
  @Override
  public String getSummary() {
    Histogram intervalHistogram = getIntervalHistogramAndAccumulate();
    // we use the summary interval as the histogram file interval.
    if(histogramLogWriter != null) {
      histogramLogWriter.outputIntervalHistogram(intervalHistogram);
    }

    DecimalFormat d = new DecimalFormat("#.##");
    return "[" + getName() +
            ": Count=" + intervalHistogram.getTotalCount() +
            ", Max=" + intervalHistogram.getMaxValue() +
            ", Min=" + intervalHistogram.getMinValue() +
            ", Avg=" + d.format(intervalHistogram.getMean()) +
            ", 90=" + d.format(intervalHistogram.getValueAtPercentile(90)) +
            ", 99=" + d.format(intervalHistogram.getValueAtPercentile(99)) +
            ", 99.9=" + d.format(intervalHistogram.getValueAtPercentile(99.9)) +
            ", 99.99=" + d.format(intervalHistogram.getValueAtPercentile(99.99)) +"]";
  }

  private Histogram getIntervalHistogramAndAccumulate() {
      Histogram intervalHistogram = histogram.getIntervalHistogram();
      // add this to the total time histogram.
      if (totalHistogram == null) {
        totalHistogram = intervalHistogram;
      }
      else {
        totalHistogram.add(intervalHistogram);
      }
      return intervalHistogram;
  }

}
