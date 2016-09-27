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

import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Collects latency measurements, and reports them when requested.
 *
 * @author cooperb
 *
 */
public class Measurements {
  /**
   * All supported measurement types are defined in this enum.
   *
   */
  public enum MeasurementType {
    HISTOGRAM,
    HDRHISTOGRAM,
    HDRHISTOGRAM_AND_HISTOGRAM,
    HDRHISTOGRAM_AND_RAW,
    TIMESERIES,
    RAW
  }

  public static final String MEASUREMENT_TYPE_PROPERTY = "measurementtype";
  private static final String MEASUREMENT_TYPE_PROPERTY_DEFAULT = "hdrhistogram";
  
  public static final String MEASUREMENT_INTERVAL = "measurement.interval";
  private static final String MEASUREMENT_INTERVAL_DEFAULT = "op";
  
  public static final String MEASUREMENT_TRACK_JVM_PROPERTY = "measurement.trackjvm";
  public static final String MEASUREMENT_TRACK_JVM_PROPERTY_DEFAULT = "false";

  static Measurements singleton=null;
  static Properties measurementproperties=null;

  public static void setProperties(Properties props)
  {
    measurementproperties=props;
  }

  /**
   * Return the singleton Measurements object.
   */
  public synchronized static Measurements getMeasurements()
  {
    if (singleton==null)
    {
      singleton=new Measurements(measurementproperties);
    }
    return singleton;
  }

  final ConcurrentHashMap<String,OneMeasurement> _opToMesurementMap;
  final ConcurrentHashMap<String,OneMeasurement> _opToIntendedMesurementMap;
  final MeasurementType _measurementType;
  final int _measurementInterval;
  private Properties _props;

  /**
   * Create a new object with the specified properties.
   */
  public Measurements(Properties props)
  {
    _opToMesurementMap=new ConcurrentHashMap<String,OneMeasurement>();
    _opToIntendedMesurementMap=new ConcurrentHashMap<String,OneMeasurement>();

    _props=props;

    String mTypeString = _props.getProperty(MEASUREMENT_TYPE_PROPERTY, MEASUREMENT_TYPE_PROPERTY_DEFAULT);
    if (mTypeString.equals("histogram"))
    {
      _measurementType = MeasurementType.HISTOGRAM;
    }
    else if (mTypeString.equals("hdrhistogram"))
    {
      _measurementType = MeasurementType.HDRHISTOGRAM;
    }
    else if (mTypeString.equals("hdrhistogram+histogram"))
    {
      _measurementType = MeasurementType.HDRHISTOGRAM_AND_HISTOGRAM;
    }
    else if (mTypeString.equals("hdrhistogram+raw"))
    {
      _measurementType = MeasurementType.HDRHISTOGRAM_AND_RAW;
    }
    else if (mTypeString.equals("timeseries"))
    {
      _measurementType = MeasurementType.TIMESERIES;
    }
    else if (mTypeString.equals("raw"))
    {
      _measurementType = MeasurementType.RAW;
    }
    else {
      throw new IllegalArgumentException("unknown "+MEASUREMENT_TYPE_PROPERTY+"="+mTypeString);
    }

    String mIntervalString = _props.getProperty(MEASUREMENT_INTERVAL, MEASUREMENT_INTERVAL_DEFAULT);
    if (mIntervalString.equals("op"))
    {
      _measurementInterval = 0;
    }
    else if (mIntervalString.equals("intended"))
    {
      _measurementInterval = 1;
    }
    else if (mIntervalString.equals("both"))
    {
      _measurementInterval = 2;
    }
    else {
      throw new IllegalArgumentException("unknown "+MEASUREMENT_INTERVAL+"="+mIntervalString);
    }
  }

  OneMeasurement constructOneMeasurement(String name)
  {
    switch (_measurementType)
    {
    case HISTOGRAM:
      return new OneMeasurementHistogram(name, _props);
    case HDRHISTOGRAM:
      return new OneMeasurementHdrHistogram(name, _props);
    case HDRHISTOGRAM_AND_HISTOGRAM:
      return new TwoInOneMeasurement(name,
              new OneMeasurementHdrHistogram("Hdr"+name, _props),
              new OneMeasurementHistogram("Bucket"+name, _props));
    case HDRHISTOGRAM_AND_RAW:
      return new TwoInOneMeasurement(name,
          new OneMeasurementHdrHistogram("Hdr"+name, _props),
          new OneMeasurementRaw("Raw"+name, _props));
    case TIMESERIES:
      return new OneMeasurementTimeSeries(name, _props);
    case RAW:
      return new OneMeasurementRaw(name, _props);
    default:
      throw new AssertionError("Impossible to be here. Dead code reached. Bugs?");
    }
  }

  static class StartTimeHolder {
    long time;

    long startTime(){
      if(time == 0) {
        return System.nanoTime();
      }
      else {
        return time;
      }
    }
  }

  ThreadLocal<StartTimeHolder> tlIntendedStartTime = new ThreadLocal<Measurements.StartTimeHolder>() {
    protected StartTimeHolder initialValue() {
      return new StartTimeHolder();
    }
  };

  public void setIntendedStartTimeNs(long time) {
    if(_measurementInterval==0)
      return;
    tlIntendedStartTime.get().time=time;
  }

  public long getIntendedtartTimeNs() {
    if(_measurementInterval==0)
      return 0L;
    return tlIntendedStartTime.get().startTime();
  }

  /**
   * Report a single value of a single metric. E.g. for read latency, operation="READ" and latency is the measured
   * value.
   */
  public void measure(String operation, int latency)
  {
    if(_measurementInterval==1)
      return;
    try
    {
      OneMeasurement m = getOpMeasurement(operation);
      m.measure(latency);
    }
    // This seems like a terribly hacky way to cover up for a bug in the measurement code
    catch (java.lang.ArrayIndexOutOfBoundsException e)
    {
      System.out.println("ERROR: java.lang.ArrayIndexOutOfBoundsException - ignoring and continuing");
      e.printStackTrace();
      e.printStackTrace(System.out);
    }
  }

  /**
   * Report a single value of a single metric. E.g. for read latency, operation="READ" and latency is the measured
   * value.
   */
  public void measureIntended(String operation, int latency)
  {
    if(_measurementInterval==0)
      return;
    try
    {
      OneMeasurement m = getOpIntendedMeasurement(operation);
      m.measure(latency);
    }
    // This seems like a terribly hacky way to cover up for a bug in the measurement code
    catch (java.lang.ArrayIndexOutOfBoundsException e)
    {
      System.out.println("ERROR: java.lang.ArrayIndexOutOfBoundsException - ignoring and continuing");
      e.printStackTrace();
      e.printStackTrace(System.out);
    }
  }

  private OneMeasurement getOpMeasurement(String operation) {
    OneMeasurement m = _opToMesurementMap.get(operation);
    if(m == null)
    {
      m = constructOneMeasurement(operation);
      OneMeasurement oldM = _opToMesurementMap.putIfAbsent(operation, m);
      if(oldM != null)
      {
          m = oldM;
      }
    }
    return m;
  }

  private OneMeasurement getOpIntendedMeasurement(String operation) {
    OneMeasurement m = _opToIntendedMesurementMap.get(operation);
    if(m == null)
    {
      final String name = _measurementInterval==1 ? operation : "Intended-" + operation;
      m = constructOneMeasurement(name);
      OneMeasurement oldM = _opToIntendedMesurementMap.putIfAbsent(operation, m);
      if(oldM != null)
      {
        m = oldM;
      }
    }
    return m;
  }

  /**
   * Report a return code for a single DB operation.
   */
  public void reportStatus(final String operation, final Status status)
  {
    OneMeasurement m = _measurementInterval==1 ?
          getOpIntendedMeasurement(operation) :
          getOpMeasurement(operation);
    m.reportStatus(status);
  }

  /**
   * Export the current measurements to a suitable format.
   *
   * @param exporter Exporter representing the type of format to write to.
   * @throws IOException Thrown if the export failed.
   */
  public void exportMeasurements(MeasurementsExporter exporter) throws IOException
  {
    for (OneMeasurement measurement : _opToMesurementMap.values())
    {
      measurement.exportMeasurements(exporter);
    }
    for (OneMeasurement measurement : _opToIntendedMesurementMap.values())
    {
      measurement.exportMeasurements(exporter);
    }
  }

  /**
   * Return a one line summary of the measurements.
   */
  public synchronized String getSummary()
  {
    String ret="";
    for (OneMeasurement m : _opToMesurementMap.values())
    {
      ret += m.getSummary()+" ";
    }
    for (OneMeasurement m : _opToIntendedMesurementMap.values())
    {
      ret += m.getSummary()+" ";
    }
    return ret;
  }

}
