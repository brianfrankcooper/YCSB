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

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;


/**
 * Take measurements and maintain a histogram of a given metric, such as READ LATENCY.
 *
 * @author cooperb
 *
 */
public class OneMeasurementHistogram extends OneMeasurement
{
  public static final String BUCKETS="histogram.buckets";
  public static final String BUCKETS_DEFAULT="1000";


  /**
   * Specify the range of latencies to track in the histogram.
   */
  int _buckets;

  /**
     * Groups operations in discrete blocks of 1ms width.
     */
  int[] histogram;
  
  /**
   * Counts all operations outside the histogram's range.
   */
  int histogramoverflow;
  
  /**
   * The total number of reported operations.
   */
  int operations;
  
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
  
  //keep a windowed version of these stats for printing status
  int windowoperations;
  long windowtotallatency;

  int min;
  int max;

  public OneMeasurementHistogram(String name, Properties props)
  {
    super(name);
    _buckets=Integer.parseInt(props.getProperty(BUCKETS, BUCKETS_DEFAULT));
    histogram=new int[_buckets];
    histogramoverflow=0;
    operations=0;
    totallatency=0;
    totalsquaredlatency=0;
    windowoperations=0;
    windowtotallatency=0;
    min=-1;
    max=-1;
  }

  /* (non-Javadoc)
   * @see com.yahoo.ycsb.OneMeasurement#measure(int)
   */
  public synchronized void measure(int latency)
  {
      //latency reported in us and collected in bucket by ms.
    if (latency/1000>=_buckets)
    {
      histogramoverflow++;
    }
    else
    {
      histogram[latency/1000]++;
    }
    operations++;
    totallatency += latency;
    totalsquaredlatency += ((double)latency) * ((double)latency);
    windowoperations++;
    windowtotallatency += latency;

    if ( (min<0) || (latency<min) )
    {
      min=latency;
    }

    if ( (max<0) || (latency>max) )
    {
      max=latency;
    }
  }

  @Override
  public void exportMeasurements(MeasurementsExporter exporter) throws IOException
  {
    double mean = totallatency/((double)operations);
    double variance = totalsquaredlatency/((double)operations) - (mean * mean);
    exporter.write(getName(), "Operations", operations);
    exporter.write(getName(), "AverageLatency(us)", mean);
    exporter.write(getName(), "LatencyVariance(us)", variance);
    exporter.write(getName(), "MinLatency(us)", min);
    exporter.write(getName(), "MaxLatency(us)", max);

    int opcounter=0;
    boolean done95th=false;
    for (int i=0; i<_buckets; i++)
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

    for (int i=0; i<_buckets; i++)
    {
      exporter.write(getName(), Integer.toString(i), histogram[i]);
    }
    exporter.write(getName(), ">"+_buckets, histogramoverflow);
  }

  @Override
  public String getSummary() {
    if (windowoperations==0)
    {
      return "";
    }
    DecimalFormat d = new DecimalFormat("#.##");
    double report=((double)windowtotallatency)/((double)windowoperations);
    windowtotallatency=0;
    windowoperations=0;
    return "["+getName()+" AverageLatency(us)="+d.format(report)+"]";
  }
}
