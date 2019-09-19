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

import site.ycsb.Status;
import site.ycsb.measurements.exporter.MeasurementsExporter;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Collects latency measurements, and reports them when requested.
 */
public class Measurements {
  /**
   * All supported measurement types are defined in this enum.
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

  private static Measurements singleton = null;
  private static Properties measurementproperties = null;

  public static void setProperties(Properties props) {
    measurementproperties = props;
  }

  /**
   * Return the singleton Measurements object.
   */
  public static synchronized Measurements getMeasurements() {
    if (singleton == null) {
      singleton = new Measurements(measurementproperties);
    }
    return singleton;
  }

  private final ConcurrentHashMap<String, OneMeasurement> opToMesurementMap;
  private final ConcurrentHashMap<String, OneMeasurement> opToIntendedMesurementMap;
  private final MeasurementType measurementType;
  private final int measurementInterval;
  private final Properties props;

  /**
   * Create a new object with the specified properties.
   */
  public Measurements(Properties props) {
    opToMesurementMap = new ConcurrentHashMap<>();
    opToIntendedMesurementMap = new ConcurrentHashMap<>();

    this.props = props;

    String mTypeString = this.props.getProperty(MEASUREMENT_TYPE_PROPERTY, MEASUREMENT_TYPE_PROPERTY_DEFAULT);
    switch (mTypeString) {
    case "histogram":
      measurementType = MeasurementType.HISTOGRAM;
      break;
    case "hdrhistogram":
      measurementType = MeasurementType.HDRHISTOGRAM;
      break;
    case "hdrhistogram+histogram":
      measurementType = MeasurementType.HDRHISTOGRAM_AND_HISTOGRAM;
      break;
    case "hdrhistogram+raw":
      measurementType = MeasurementType.HDRHISTOGRAM_AND_RAW;
      break;
    case "timeseries":
      measurementType = MeasurementType.TIMESERIES;
      break;
    case "raw":
      measurementType = MeasurementType.RAW;
      break;
    default:
      throw new IllegalArgumentException("unknown " + MEASUREMENT_TYPE_PROPERTY + "=" + mTypeString);
    }

    String mIntervalString = this.props.getProperty(MEASUREMENT_INTERVAL, MEASUREMENT_INTERVAL_DEFAULT);
    switch (mIntervalString) {
    case "op":
      measurementInterval = 0;
      break;
    case "intended":
      measurementInterval = 1;
      break;
    case "both":
      measurementInterval = 2;
      break;
    default:
      throw new IllegalArgumentException("unknown " + MEASUREMENT_INTERVAL + "=" + mIntervalString);
    }
  }

  private OneMeasurement constructOneMeasurement(String name) {
    switch (measurementType) {
    case HISTOGRAM:
      return new OneMeasurementHistogram(name, props);
    case HDRHISTOGRAM:
      return new OneMeasurementHdrHistogram(name, props);
    case HDRHISTOGRAM_AND_HISTOGRAM:
      return new TwoInOneMeasurement(name,
          new OneMeasurementHdrHistogram("Hdr" + name, props),
          new OneMeasurementHistogram("Bucket" + name, props));
    case HDRHISTOGRAM_AND_RAW:
      return new TwoInOneMeasurement(name,
          new OneMeasurementHdrHistogram("Hdr" + name, props),
          new OneMeasurementRaw("Raw" + name, props));
    case TIMESERIES:
      return new OneMeasurementTimeSeries(name, props);
    case RAW:
      return new OneMeasurementRaw(name, props);
    default:
      throw new AssertionError("Impossible to be here. Dead code reached. Bugs?");
    }
  }

  static class StartTimeHolder {
    protected long time;

    long startTime() {
      if (time == 0) {
        return System.nanoTime();
      } else {
        return time;
      }
    }
  }

  private final ThreadLocal<StartTimeHolder> tlIntendedStartTime = new ThreadLocal<Measurements.StartTimeHolder>() {
    protected StartTimeHolder initialValue() {
      return new StartTimeHolder();
    }
  };

  public void setIntendedStartTimeNs(long time) {
    if (measurementInterval == 0) {
      return;
    }
    tlIntendedStartTime.get().time = time;
  }

  public long getIntendedtartTimeNs() {
    if (measurementInterval == 0) {
      return 0L;
    }
    return tlIntendedStartTime.get().startTime();
  }

  /**
   * Report a single value of a single metric. E.g. for read latency, operation="READ" and latency is the measured
   * value.
   */
  public void measure(String operation, int latency) {
    if (measurementInterval == 1) {
      return;
    }
    try {
      OneMeasurement m = getOpMeasurement(operation);
      m.measure(latency);
    } catch (java.lang.ArrayIndexOutOfBoundsException e) {
      // This seems like a terribly hacky way to cover up for a bug in the measurement code
      System.out.println("ERROR: java.lang.ArrayIndexOutOfBoundsException - ignoring and continuing");
      e.printStackTrace();
      e.printStackTrace(System.out);
    }
  }

  /**
   * Report a single value of a single metric. E.g. for read latency, operation="READ" and latency is the measured
   * value.
   */
  public void measureIntended(String operation, int latency) {
    if (measurementInterval == 0) {
      return;
    }
    try {
      OneMeasurement m = getOpIntendedMeasurement(operation);
      m.measure(latency);
    } catch (java.lang.ArrayIndexOutOfBoundsException e) {
      // This seems like a terribly hacky way to cover up for a bug in the measurement code
      System.out.println("ERROR: java.lang.ArrayIndexOutOfBoundsException - ignoring and continuing");
      e.printStackTrace();
      e.printStackTrace(System.out);
    }
  }

  private OneMeasurement getOpMeasurement(String operation) {
    OneMeasurement m = opToMesurementMap.get(operation);
    if (m == null) {
      m = constructOneMeasurement(operation);
      OneMeasurement oldM = opToMesurementMap.putIfAbsent(operation, m);
      if (oldM != null) {
        m = oldM;
      }
    }
    return m;
  }

  private OneMeasurement getOpIntendedMeasurement(String operation) {
    OneMeasurement m = opToIntendedMesurementMap.get(operation);
    if (m == null) {
      final String name = measurementInterval == 1 ? operation : "Intended-" + operation;
      m = constructOneMeasurement(name);
      OneMeasurement oldM = opToIntendedMesurementMap.putIfAbsent(operation, m);
      if (oldM != null) {
        m = oldM;
      }
    }
    return m;
  }

  /**
   * Report a return code for a single DB operation.
   */
  public void reportStatus(final String operation, final Status status) {
    OneMeasurement m = measurementInterval == 1 ?
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
  public void exportMeasurements(MeasurementsExporter exporter) throws IOException {
    for (OneMeasurement measurement : opToMesurementMap.values()) {
      measurement.exportMeasurements(exporter);
    }
    for (OneMeasurement measurement : opToIntendedMesurementMap.values()) {
      measurement.exportMeasurements(exporter);
    }
  }

  /**
   * Return a one line summary of the measurements.
   */
  public synchronized String getSummary() {
    String ret = "";
    for (OneMeasurement m : opToMesurementMap.values()) {
      ret += m.getSummary() + " ";
    }
    for (OneMeasurement m : opToIntendedMesurementMap.values()) {
      ret += m.getSummary() + " ";
    }
    return ret;
  }

}
