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
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;

/**
 * Collects latency measurements, and reports them when requested.
 *
 * @author cooperb
 *
 */
public class Measurements {

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
  private final int measurementType;
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

    final String mTypeString = props.getProperty(MEASUREMENT_TYPE_PROPERTY,
        MEASUREMENT_TYPE_PROPERTY_DEFAULT);
    if (mTypeString.equals("histogram")) {
      measurementType = 0;
    } else if (mTypeString.equals("hdrhistogram")) {
      measurementType = 1;
    } else if (mTypeString.equals("hdrhistogram+histogram")) {
      measurementType = 2;
    } else if (mTypeString.equals("timeseries")) {
      measurementType = 3;
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
    for (final OneMeasurement measurement : opToIntendedMesurement
        .values()) {
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
  public void reportReturnCode(final String operation, final int code) {
    final OneMeasurement m = measurementInterval == 1
        ? getOpIntendedMeasurement(operation) : getOpMeasurement(operation);
    m.reportReturnCode(code);
  }

  public void setIntendedStartTimeNs(final long time) {
    if (measurementInterval == 0) {
      return;
    }
    tlIntendedStartTime.get().time = time;
  }

  OneMeasurement constructOneMeasurement(final String name) {
    switch (measurementType) {
    case 0:
      return new OneMeasurementHistogram(name, props);
    case 1:
      return new OneMeasurementHdrHistogram(name, props);
    case 2:
      return new TwoInOneMeasurement(name,
          new OneMeasurementHdrHistogram("Hdr" + name, props),
          new OneMeasurementHistogram("Bucket" + name, props));
    default:
      return new OneMeasurementTimeSeries(name, props);
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
