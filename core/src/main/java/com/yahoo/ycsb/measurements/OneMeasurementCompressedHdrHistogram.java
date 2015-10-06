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
  PrintStream filelog;
  HistogramLogWriter histogramLogWriter;
  HistogramLogWriter histogramLogFileWriter;
  ByteArrayOutputStream compressedHistogram = new ByteArrayOutputStream();
  final Recorder histogram;
  Histogram totalHistogram;

  public OneMeasurementCompressedHdrHistogram(String name, Properties props) {
    super(name);
    // log compressed histogram to ByteArrayOutputStream
    try {
      log = new PrintStream(compressedHistogram, false, "UTF-8");
      histogramLogWriter = new HistogramLogWriter(log);
      long now = System.currentTimeMillis();
      histogramLogWriter.setBaseTime(now);
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }

    // log to file?
    boolean shouldLog = Boolean.parseBoolean(props.getProperty("hdrhistogram.fileoutput", "false"));
    if (!shouldLog) {
      filelog = null;
      histogramLogFileWriter = null;
    } else {
      try {
        final String hdrOutputFilename = props.getProperty("hdrhistogram.output.path", "") + name + ".hdr";
        filelog = new PrintStream(new FileOutputStream(hdrOutputFilename), false);
      } catch (FileNotFoundException e) {
        throw new RuntimeException("Failed to open hdr histogram output file", e);
      }
      histogramLogFileWriter = new HistogramLogWriter(filelog);
      histogramLogFileWriter.outputComment("[Logging for: " + name + "]");
      histogramLogFileWriter.outputLogFormatVersion();
      long now = System.currentTimeMillis();
      histogramLogFileWriter.outputStartTime(now);
      histogramLogFileWriter.setBaseTime(now);
      histogramLogFileWriter.outputLegend();
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
    if(histogramLogFileWriter != null) {
      histogramLogFileWriter.outputIntervalHistogram(intervalHistogram);
      // we can close now
      filelog.close();
    }
    exporter.write(getName(), "Operations", totalHistogram.getTotalCount());
    exporter.write(getName(), "AverageLatency(us)", totalHistogram.getMean());
    exporter.write(getName(), "MinLatency(us)", totalHistogram.getMinValue());
    exporter.write(getName(), "MaxLatency(us)", totalHistogram.getMaxValue());
    exporter.write(getName(), "95thPercentileLatency(ms)", totalHistogram.getValueAtPercentile(90)/1000);
    exporter.write(getName(), "99thPercentileLatency(ms)", totalHistogram.getValueAtPercentile(99)/1000);
    exporter.write(getName(), "99.9thPercentileLatency(ms)", totalHistogram.getValueAtPercentile(99.9)/1000);
    exporter.write(getName(), "99.99thPercentileLatency(ms)", totalHistogram.getValueAtPercentile(99.99)/1000);

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

    if (histogramLogWriter != null) {
      histogramLogWriter.outputIntervalHistogram(intervalHistogram);
    }

    String summary = "";
    try {
      summary = compressedHistogram.toString("UTF8");

    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    return summary;
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
