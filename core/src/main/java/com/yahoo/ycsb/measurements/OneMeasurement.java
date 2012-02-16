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

import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;

/**
 * A single measured metric (such as READ LATENCY)
 */
public abstract class OneMeasurement {

	String _name;
	
	public String getName() {
		return _name;
	}

	/**
	 * @param _name
	 */
	public OneMeasurement(String _name) {
		this._name = _name;
	}

	public abstract void reportReturnCode(int code);

	public abstract void measure(int latency);

	public abstract String getSummary();

  /**
   * Export the current measurements to a suitable format.
   * 
   * @param exporter Exporter representing the type of format to write to.
   * @throws IOException Thrown if the export failed.
   */
  public abstract void exportMeasurements(MeasurementsExporter exporter) throws IOException;
}
