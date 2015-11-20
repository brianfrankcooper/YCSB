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
import java.util.Vector;
import java.util.Properties;
import java.text.DecimalFormat;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;

import com.yahoo.ycsb.measurements.SeriesUnit;

/**
 * A time series measurement of a metric, such as READ LATENCY.
 */
public class OneMeasurementTimeSeriesOps extends OneMeasurement
{

  public static final String INTERVAL="timeseries.intervalSecs";
  public static final String INTERVAL_DEFAULT="1";

  int _intervalMS;
  Vector<SeriesUnit> _measurements;

  long start=-1;
  long currentunit=-1;
  int count=0;
  int sum=0;
  int operations=0;
  long totallatency=0;

  //keep a windowed version of these stats for printing status
  int windowoperations=0;
  long windowtotallatency=0;

  int min=-1;
  int max=-1;

  public OneMeasurementTimeSeriesOps(String name, Properties props)
  {
    super(name);
    _intervalMS=1000*Integer.parseInt(props.getProperty(INTERVAL,INTERVAL_DEFAULT));
    _measurements=new Vector<SeriesUnit>();
  }

  void checkEndOfUnit(boolean forceend)
  {
    long now=System.currentTimeMillis();

    if (start<0)
    {
      currentunit=0;
      start=now;
    }

    // We want output units to be seconds. sorry for the mess...
    long unit=((now-start)/_intervalMS)*(_intervalMS/1000);

    if ( (unit>currentunit) || (forceend) )
    {
      // Average latency
      double avg=((double)sum)/((double)count);
      _measurements.add(new SeriesUnit(unit,avg,count));

      currentunit=unit;

      count=0;
      sum=0;
    }
  }

  @Override
  public void measure(int latency)
  {
    checkEndOfUnit(false);

    count++;
    sum+=latency;
    totallatency+=latency;
    operations++;
    windowoperations++;
    windowtotallatency+=latency;

    if (latency>max)
    {
      max=latency;
    }

    if ( (latency<min) || (min<0) )
    {
      min=latency;
    }
  }


  @Override
  public void exportMeasurements(MeasurementsExporter exporter) throws IOException {
    checkEndOfUnit(true);

    exporter.write(getName(), "Operations", operations);
    exporter.write(getName(), "AverageLatency(us)", (((double) totallatency) / ((double) operations)));
    exporter.write(getName(), "MinLatency(us)", min);
    exporter.write(getName(), "MaxLatency(us)", max);

    // TODO: 95th and 99th percentile latency

    exportStatusCounts(exporter);
    for (SeriesUnit unit : _measurements) {
      // Granularity of 1 sec is good enough for me
      exporter.write("at time, ops", Long.toString(unit.time), unit.count);
    }
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
