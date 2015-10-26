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

/**
 * Delegates to 2 measurement instances.
 * 
 * @author nitsanw
 */
public class TwoInOneMeasurement extends OneMeasurement {

  private final OneMeasurement thing1;
  private final OneMeasurement thing2;

  public TwoInOneMeasurement(final String name, final OneMeasurement thing1,
      final OneMeasurement thing2) {
    super(name);
    this.thing1 = thing1;
    this.thing2 = thing2;
  }

  /**
   * This is called from a main thread, on orderly termination.
   *
   * @see OneMeasurement#exportMeasurements(MeasurementsExporter)
   */
  @Override
  public void exportMeasurements(final MeasurementsExporter exporter)
      throws IOException {
    thing1.exportMeasurements(exporter);
    thing2.exportMeasurements(exporter);
  }

  /**
   * This is called periodically from the StatusThread. There's a single
   * StatusThread per Client process. We optionally serialize the interval to
   * log on this opportunity.
   * 
   * @see OneMeasurement#getSummary()
   */
  @Override
  public String getSummary() {
    return thing1.getSummary() + "\n" + thing2.getSummary();
  }

  /**
   * Updates all of the measurements.
   *
   * @see OneMeasurement#measure(int)
   */
  @Override
  public void measure(final int latencyInMicros) {
    thing1.measure(latencyInMicros);
    thing2.measure(latencyInMicros);
  }

  /**
   * Returns the result from the first measurement.
   *
   * @see OneMeasurement#reportStatus(Status)
   */
  @Override
  public void reportStatus(final Status status) {
    thing1.reportStatus(status);
  }

}
