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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;

/**
 * A single measured metric (such as READ LATENCY).
 */
public abstract class OneMeasurement {

  private final String name;
  private final ConcurrentHashMap<Integer, AtomicInteger> returncodes;

  /**
   * @param _name
   */
  public OneMeasurement(final String name) {
    this.name = name;
    this.returncodes = new ConcurrentHashMap<Integer, AtomicInteger>();
  }

  /**
   * Export the current measurements to a suitable format.
   *
   * @param exporter
   *          Exporter representing the type of format to write to.
   * @throws IOException
   *           Thrown if the export failed.
   */
  public abstract void exportMeasurements(MeasurementsExporter exporter)
      throws IOException;

  public String getName() {
    return name;
  }

  public abstract String getSummary();

  public abstract void measure(int latency);

  /**
   * No need for synchronization, using CHM to deal with that.
   */
  public void reportReturnCode(final int code) {
    final Integer iCode = code;
    
    AtomicInteger counter = returncodes.get(iCode);
    if (counter == null) {
      counter = new AtomicInteger();
      final AtomicInteger other = returncodes.putIfAbsent(iCode, counter);
      if (other != null) {
        counter = other;
      }
    }

    counter.incrementAndGet();
  }

  protected final void exportReturnCodes(final MeasurementsExporter exporter)
      throws IOException {
    for (final Map.Entry<Integer, AtomicInteger> entry : returncodes
        .entrySet()) {
      exporter.write(getName(), "Return=" + entry.getKey(),
          entry.getValue().get());
    }
  }
}
