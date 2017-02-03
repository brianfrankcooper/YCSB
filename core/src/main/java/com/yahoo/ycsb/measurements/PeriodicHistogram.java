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

import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Properties;

import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;

/**
 * Take measurements and maintain a histogram of a given metric, such as READ LATENCY.
 * Print out this histogram at periodic intervals.
 * 
 * @author cooperb
 */
public class PeriodicHistogram extends OneMeasurement
{
  public static final String BUCKETS = "periodichistogram.buckets";
  public static final String BUCKETS_DEFAULT = "1000";
  
  /**
   * Latency bucket interval in milliseconds.
   * 1000  = 1 second per bucket
   * 100   = 100ms per bucket
   * 10    = 10ms per bucket
   * 1     = 1ms per bucket (default)
   * 0.1   = 100us per bucket
   * 0.01  = 10us per bucket 
   * 0.001 = 1us per bucket 
   */
  public static final String BUCKET_INTERVAL = "periodichistogram.bucket.interval";
  public static final String BUCKET_INTERVAL_DEFAULT = "1.0";

  public static final String OUTPUT_DIR = "periodichistogram.output.path";
  public static final String OUTPUT_DIR_DEFAULT = "";
  
  private static String NEWLINE = System.getProperty("line.separator");
  
  /**
   * Specify the range of latencies to track in the histogram.
   */
  final int buckets;
	
  /**
   * Groups operations in discrete blocks of 1ms width.
   */
  int[] histogram;
  
  /**
   * Histogram of previous status thread iteration.
   */
  int[] prevhistogram;
  
  /**
   * Counts all operations outside the histogram's range.
   */
  int histogramoverflow;
  
  /**
   * Overflow of previous status thread iteration.
   */
  int prevhistogramoverflow;
  
  /**
   * The total number of reported operations.
   */
  int operations;
  
  /**
   * Reported operations from previous status thread iteration.
   */
  int prevoperations;
  
  /**
   * The latency bucket multiplier used to convert latency in microseconds to the proper bucket index.
   */
  double latencymultiplier;

  /**
   * The sum of each latency measurement over all operations.
   * Calculated in ms.
   */
  long totallatency;

  /**
   * The sum of each latency measurement squared over all operations. 
   * Used to calculate variance of latency.
   * Calculated in ms. 
   */
  double totalsquaredlatency;
  
  /**
   * Start of status interval in milliseconds since epoch.
   */
  long windowstart;
  
  /**
   * End of status interval in milliseconds since epoch.
   */
  long windowend;

  /**
   * Minimum latency for entire run.
   */
  int min;
  
  /**
   * Maximum latency for entire run.
   */
  int max;
  
  /**
   * Number of seconds in each bucket expressed as a string.  Used in json output.
   */
  final String secondsperbucketstring;

  /**
   * File path of periodic json histogram.
   */
  String path;
  
  /**
   * File writer for periodic json histogram.
   */
  FileWriter writer;
  
  /**
   * Reusable string builder.
   */
  StringBuilder sb;
  
  /**
   * Floating point formatter.
   */
  DecimalFormat secondsFormat;
  DecimalFormat opsecFormat;

  public PeriodicHistogram(String name, Properties props)
  {
    super(name);

    buckets = Integer.parseInt(props.getProperty(BUCKETS, BUCKETS_DEFAULT));
    double bucketinterval = Double.parseDouble(props.getProperty(BUCKET_INTERVAL, BUCKET_INTERVAL_DEFAULT));
    latencymultiplier = 1.0 / (bucketinterval * 1000.0);
    secondsperbucketstring = new DecimalFormat("#.############").format(bucketinterval / 1000.0);

    histogram = new int[buckets];
    prevhistogram = new int[buckets];
    histogramoverflow = 0;
    prevhistogramoverflow = 0;
    operations = 0;
    prevoperations = 0;
    totallatency = 0;
    totalsquaredlatency = 0;
    min = -1;
    max = -1;
    windowstart = windowend = System.currentTimeMillis();
    
    path = props.getProperty(OUTPUT_DIR, OUTPUT_DIR_DEFAULT) + name + ".json";

    try
    {
      writer = new FileWriter(path);
    }
    catch (IOException ioe)
    {
      throw new RuntimeException("Failed to create output file: " + path);
    }

    sb = new StringBuilder(1000);
    secondsFormat = new DecimalFormat("#.###");
    opsecFormat = new DecimalFormat("#.##");
  }

  /* (non-Javadoc)
   * @see com.yahoo.ycsb.OneMeasurement#measure(int)
   */
  public synchronized void measure(int latency)
  {
    // latency reported in us and collected in bucket by bucket interval.
    int bucket = (int)(latency * latencymultiplier);

    if (bucket >= buckets)
    {
      histogramoverflow++;
    }
    else
    {
      histogram[bucket]++;
    }
    operations++;
    totallatency += latency;
    totalsquaredlatency += ((double)latency) * ((double)latency);

    if ((min < 0) || (latency < min))
    {
      min = latency;
    }

    if ((max < 0) || (latency > max))
    {
      max = latency;
    }
  }

  @Override
  public void exportMeasurements(MeasurementsExporter exporter) throws IOException
  {
    writejson();
    writer.close();

    double mean = totallatency/((double)operations);
    double variance = totalsquaredlatency/((double)operations) - (mean * mean);
    exporter.write(getName(), "Operations", operations);
    exporter.write(getName(), "AverageLatency(us)", mean);
    exporter.write(getName(), "LatencyVariance(us)", variance);
    exporter.write(getName(), "MinLatency(us)", min);
    exporter.write(getName(), "MaxLatency(us)", max);

    int opcounter=0;
    boolean done95th=false;
    for (int i=0; i<buckets; i++)
    {
      opcounter+=histogram[i];
      if ( (!done95th) && (((double)opcounter)/((double)operations)>=0.95) )
      {
        exporter.write(getName(), "95thPercentileLatency(us)", i*1000);
        done95th=true;
      }
      if (((double)opcounter)/((double)operations)>=0.99)
      {
        exporter.write(getName(), "99thPercentileLatency(us)", i*1000);
        break;
      }
    }

    exportStatusCounts(exporter);

    for (int i=0; i<buckets; i++)
    {
      exporter.write(getName(), Integer.toString(i), histogram[i]);
    }
    exporter.write(getName(), ">"+buckets, histogramoverflow);  
  }

  @Override
  public String getSummary() 
  {
    writejson();    
    return "";
  }
  
  private void writejson()
  {
    int count = operations;
    int diff = count - prevoperations;
    prevoperations = count;
    
    if (diff <= 0)
    {
      windowstart = windowend = System.currentTimeMillis();
      return;
    }
    windowend = System.currentTimeMillis();
    
    sb.setLength(0);
    sb.append("{\"start\":");
    sb.append(secondsFormat.format((double)windowstart / 1000.0));
    sb.append(", \"end\":");
    sb.append(secondsFormat.format((double)windowend / 1000.0));
    sb.append(", \"opsec\":");
    sb.append(opsecFormat.format((double)diff / (double)(windowend - windowstart) * 1000.0));
    sb.append(", \"scale\":");
    sb.append(secondsperbucketstring);
    sb.append(", \"linear\":{");
    windowstart = windowend;
    
    boolean comma = false;
  
    for (int i = 0; i < buckets; i++)
    {
      count = histogram[i];
      diff = count - prevhistogram[i];
      prevhistogram[i] = count;
	
      if (diff > 0)
      {
        if (comma) 
        {
          sb.append(", ");
        }
        else
        {
          comma = true;
        }
        sb.append('"');
        sb.append(i);
        sb.append("\":");
        sb.append(diff);
      }
    }

    count = histogramoverflow;
    diff = histogramoverflow - prevhistogramoverflow;
    prevhistogramoverflow = count;

    if (diff > 0)
    {
      if (comma) 
      {
        sb.append(", ");
      }
      else
      {
        comma = true;
      }
      sb.append("\">");
      sb.append(buckets);
      sb.append("\":");
      sb.append(diff);
    }    
    sb.append("}}");
    sb.append(NEWLINE);
    
    try
    {
      writer.write(sb.toString());
      writer.flush();
    }
    catch (Exception e)
    {
      throw new RuntimeException("Failed to write histogram: " + path);
    }
  }
}
