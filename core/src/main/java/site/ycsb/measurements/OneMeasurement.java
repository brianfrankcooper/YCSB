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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A single measured metric (such as READ LATENCY).
 */
public abstract class OneMeasurement {

  private final String name;
  private final ConcurrentHashMap<Status, AtomicInteger> returncodes;

  public String getName() {
    return name;
  }

  /**
   * @param name measurement name
   */
  public OneMeasurement(String name) {
    this.name = name;
    this.returncodes = new ConcurrentHashMap<>();
  }

  public abstract void measure(int latency);

  public abstract String getSummary();

  /**
   * No need for synchronization, using CHM to deal with that.
   */
  public void reportStatus(Status status) {
    AtomicInteger counter = returncodes.get(status);

    if (counter == null) {
      counter = new AtomicInteger();
      AtomicInteger other = returncodes.putIfAbsent(status, counter);
      if (other != null) {
        counter = other;
      }
    }

    counter.incrementAndGet();
  }

  /**
   * Export the current measurements to a suitable format.
   *
   * @param exporter Exporter representing the type of format to write to.
   * @throws IOException Thrown if the export failed.
   */
  public abstract void exportMeasurements(MeasurementsExporter exporter) throws IOException;

  protected final void exportStatusCounts(MeasurementsExporter exporter) throws IOException {
    for (Map.Entry<Status, AtomicInteger> entry : returncodes.entrySet()) {
      exporter.write(getName(), "Return=" + entry.getKey().getName(), entry.getValue().get());
    }
  }
}
