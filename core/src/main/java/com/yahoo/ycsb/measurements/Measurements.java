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
import java.util.concurrent.ConcurrentMap;

/**
 * Collects latency measurements, and reports them when requested.
 *
 * @author cooperb
 *
 */
public class Measurements {
  /**
   * All supported measurement types are defined in this enum.
   */
  public enum MeasurementType {
    HISTOGRAM, HDRHISTOGRAM, HDRHISTOGRAM_AND_HISTOGRAM, HDRHISTOGRAM_AND_RAW, 
    TIMESERIES, RAW
  }

  public static final String MEASUREMENT_INTERVAL = "measurement.interval";
  public static final String MEASUREMENT_TYPE_PROPERTY = "measurementtype";

  private static Properties measurementproperties = null;
  private static Measurements singleton = null;

  private static final String MEASUREMENT_INTERVAL_DEFAULT = "op";
  private static final String MEASUREMENT_TYPE_PROPERTY_DEFAULT =
      "hdrhistogram";

  /**
   * Return the singleton Measurements object.
   */
  public static synchronized Measurements getMeasurements() {
    if (singleton == null) {
      singleton = new Measurements(measurementproperties);
    }
    return singleton;
  }

  public static void setProperties(final Properties props) {
    measurementproperties = props;
  }

  private final int measurementInterval;
  private final MeasurementType measurementType;
  private final ConcurrentMap<String, OneMeasurement> opToIntendedMesurement;
  private final ConcurrentMap<String, OneMeasurement> opToMesurement;
  private final ThreadLocal<StartTimeHolder> tlIntendedStartTime =
      new ThreadLocal<Measurements.StartTimeHolder>() {
        @Override
        protected StartTimeHolder initialValue() {
          return new StartTimeHolder();
        };
      };

  private final Properties props;

  /**
   * Create a new object with the specified properties.
   */
  public Measurements(final Properties props) {
    opToMesurement = new ConcurrentHashMap<String, OneMeasurement>();
    opToIntendedMesurement = new ConcurrentHashMap<String, OneMeasurement>();

    this.props = props;

    String mTypeString = props.getProperty(MEASUREMENT_TYPE_PROPERTY,
        MEASUREMENT_TYPE_PROPERTY_DEFAULT);
    if (mTypeString.equals("histogram")) {
      measurementType = MeasurementType.HISTOGRAM;
    } else if (mTypeString.equals("hdrhistogram")) {
      measurementType = MeasurementType.HDRHISTOGRAM;
    } else if (mTypeString.equals("hdrhistogram+histogram")) {
      measurementType = MeasurementType.HDRHISTOGRAM_AND_HISTOGRAM;
    } else if (mTypeString.equals("hdrhistogram+raw")) {
      measurementType = MeasurementType.HDRHISTOGRAM_AND_RAW;
    } else if (mTypeString.equals("timeseries")) {
      measurementType = MeasurementType.TIMESERIES;
    } else if (mTypeString.equals("raw")) {
      measurementType = MeasurementType.RAW;
    } else {
      throw new IllegalArgumentException(
          "unknown " + MEASUREMENT_TYPE_PROPERTY + "=" + mTypeString);
    }

    final String mIntervalString =
        props.getProperty(MEASUREMENT_INTERVAL, MEASUREMENT_INTERVAL_DEFAULT);
    if (mIntervalString.equals("op")) {
      measurementInterval = 0;
    } else if (mIntervalString.equals("intended")) {
      measurementInterval = 1;
    } else if (mIntervalString.equals("both")) {
      measurementInterval = 2;
    } else {
      throw new IllegalArgumentException(
          "unknown " + MEASUREMENT_INTERVAL + "=" + mIntervalString);
    }
  }

  /**
   * Export the current measurements to a suitable format.
   *
   * @param exporter
   *          Exporter representing the type of format to write to.
   * @throws IOException
   *           Thrown if the export failed.
   */
  public void exportMeasurements(final MeasurementsExporter exporter)
      throws IOException {
    for (final OneMeasurement measurement : opToMesurement.values()) {
      measurement.exportMeasurements(exporter);
    }
    for (final OneMeasurement measurement : opToIntendedMesurement.values()) {
      measurement.exportMeasurements(exporter);
    }
  }

  public long getIntendedtartTimeNs() {
    if (measurementInterval == 0) {
      return 0L;
    }
    return tlIntendedStartTime.get().startTime();
  }

  /**
   * Return a one line summary of the measurements.
   */
  public synchronized String getSummary() {
    String ret = "";
    for (final OneMeasurement m : opToMesurement.values()) {
      ret += m.getSummary() + " ";
    }
    for (final OneMeasurement m : opToIntendedMesurement.values()) {
      ret += m.getSummary() + " ";
    }
    return ret;
  }

  /**
   * Report a single value of a single metric. E.g. for read latency,
   * operation="READ" and latency is the measured value.
   */
  public void measure(final String operation, final int latency) {
    if (measurementInterval == 1) {
      return;
    }
    try {
      final OneMeasurement m = getOpMeasurement(operation);
      m.measure(latency);
    } catch (final java.lang.ArrayIndexOutOfBoundsException e) {
      // This seems like a terribly hacky way to cover up for a bug in the
      // measurement code.
      System.out.println("ERROR: java.lang.ArrayIndexOutOfBoundsException -"
          + " ignoring and continuing");
      e.printStackTrace();
      e.printStackTrace(System.out);
    }
  }

  /**
   * Report a single value of a single metric. E.g. for read latency,
   * operation="READ" and latency is the measured value.
   */
  public void measureIntended(final String operation, final int latency) {
    if (measurementInterval == 0) {
      return;
    }
    try {
      final OneMeasurement m = getOpIntendedMeasurement(operation);
      m.measure(latency);
    } catch (final java.lang.ArrayIndexOutOfBoundsException e) {
      // This seems like a terribly hacky way to cover up for a bug in the
      // measurement code
      System.out.println("ERROR: java.lang.ArrayIndexOutOfBoundsException - "
          + "ignoring and continuing");
      e.printStackTrace();
      e.printStackTrace(System.out);
    }
  }

  /**
   * Report a return code for a single DB operation.
   */
  public void reportStatus(final String operation, final Status status) {
    OneMeasurement m = measurementInterval == 1
        ? getOpIntendedMeasurement(operation) : getOpMeasurement(operation);
    m.reportStatus(status);
  }

  public void setIntendedStartTimeNs(final long time) {
    if (measurementInterval == 0) {
      return;
    }
    tlIntendedStartTime.get().time = time;
  }

  OneMeasurement constructOneMeasurement(String name) {
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
          new OneMeasurementHistogram("Raw" + name, props));
    case TIMESERIES:
      return new OneMeasurementTimeSeries(name, props);
    case RAW:
      return new OneMeasurementRaw(name, props);
    default:
      throw new AssertionError(
          "Impossible to be here. Dead code reached. Bugs?");
    }
  }

  private OneMeasurement getOpIntendedMeasurement(final String operation) {
    OneMeasurement m = opToIntendedMesurement.get(operation);
    if (m == null) {
      final String name =
          measurementInterval == 1 ? operation : "Intended-" + operation;
      m = constructOneMeasurement(name);
      final OneMeasurement oldM =
          opToIntendedMesurement.putIfAbsent(operation, m);
      if (oldM != null) {
        m = oldM;
      }
    }
    return m;
  }

  private OneMeasurement getOpMeasurement(final String operation) {
    OneMeasurement m = opToMesurement.get(operation);
    if (m == null) {
      m = constructOneMeasurement(operation);
      final OneMeasurement oldM = opToMesurement.putIfAbsent(operation, m);
      if (oldM != null) {
        m = oldM;
      }
    }
    return m;
  }

  static class StartTimeHolder {
    private long time;

    long startTime() {
      if (time == 0) {
        return System.nanoTime();
      } else {
        return time;
      }
    }
  }

}
