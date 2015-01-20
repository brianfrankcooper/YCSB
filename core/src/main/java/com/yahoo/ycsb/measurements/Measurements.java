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

import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;

/**
 * Collects latency measurements, and reports them when requested.
 * 
 * @author cooperb
 *
 */
public class Measurements
{
	private static final String MEASUREMENT_TYPE = "measurementtype";

	private static final String MEASUREMENT_TYPE_DEFAULT = "histogram";

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

    final ConcurrentHashMap<String,OneMeasurement> opToMesurementMap;
    final ConcurrentHashMap<String,OneMeasurement> opToIntendedMesurementMap;
	final int measurementType;

	private Properties _props;
	
      /**
       * Create a new object with the specified properties.
       */
	public Measurements(Properties props)
	{
        opToMesurementMap=new ConcurrentHashMap<String,OneMeasurement>();
        opToIntendedMesurementMap=new ConcurrentHashMap<String,OneMeasurement>();
		
		_props=props;
		
		String mTypeString = _props.getProperty(MEASUREMENT_TYPE, MEASUREMENT_TYPE_DEFAULT);
        if (mTypeString.equals("histogram"))
		{
		    measurementType=0;
		}
		else if (mTypeString.equals("hdrhistogram"))
		{
            measurementType=1;
        }
		else if (mTypeString.equals("hdrhistogram+buckethistogram"))
        {
            measurementType=2;
        }
		else
		{
		    measurementType=3;
		}
	}
	
    OneMeasurement constructOneMeasurement(String name)
    {
        switch (measurementType)
        {
        case 0:
            return new OneMeasurementHistogram(name, _props);
        case 1:
            return new OneMeasurementHdrHistogram(name, _props);
        case 2:
            return new TwoInOneMeasurement(name, 
                    new OneMeasurementHdrHistogram("Hdr"+name, _props), 
                    new OneMeasurementHistogram("Bucket"+name, _props));
        default:
            return new OneMeasurementTimeSeries(name, _props);
        }
    }

    static class StartTimeHolder{
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
    ThreadLocal<StartTimeHolder> tls = new ThreadLocal<Measurements.StartTimeHolder>(){
      protected StartTimeHolder initialValue() {
          return new StartTimeHolder();
      };  
    };
    public void setIntendedStartTimeNs(long time){
        tls.get().time=time;
    }
    
    public long getIntendedtartTimeNs(){
        return tls.get().startTime();
    }

    /**
     * Report a single value of a single metric. E.g. for read latency, operation="READ" and latency is the measured
     * value.
     */
	public void measure(String operation, int latency)
	{
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
        OneMeasurement m = opToMesurementMap.get(operation);
        if(m == null)
        {
            m = constructOneMeasurement(operation);
            OneMeasurement oldM = opToMesurementMap.putIfAbsent(operation, m);
            if(oldM != null)
            {
                m = oldM;
            }
        }
        return m;
    }
    private OneMeasurement getOpIntendedMeasurement(String operation) {
        OneMeasurement m = opToIntendedMesurementMap.get(operation);
        if(m == null)
        {
            m = constructOneMeasurement("Intended-"+operation);
            OneMeasurement oldM = opToIntendedMesurementMap.putIfAbsent(operation, m);
            if(oldM != null)
            {
                m = oldM;
            }
        }
        return m;
    }
      /**
       * Report a return code for a single DB operaiton.
       */
	public void reportReturnCode(String operation, int code)
	{
	    OneMeasurement m = getOpMeasurement(operation);
		m.reportReturnCode(code);
	}
	
  /**
   * Export the current measurements to a suitable format.
   * 
   * @param exporter Exporter representing the type of format to write to.
   * @throws IOException Thrown if the export failed.
   */
  public void exportMeasurements(MeasurementsExporter exporter) throws IOException
  {
      for (OneMeasurement measurement : opToMesurementMap.values())
      {
        measurement.exportMeasurements(exporter);
      }
      for (OneMeasurement measurement : opToIntendedMesurementMap.values())
      {
        measurement.exportMeasurements(exporter);
      }
  }
	
      /**
       * Return a one line summary of the measurements.
       */
	public String getSummary()
	{
		String ret="";
		for (OneMeasurement m : opToMesurementMap.values())
		{
			ret+=m.getSummary()+" ";
		}
		for (OneMeasurement m : opToIntendedMesurementMap.values())
        {
            ret+=m.getSummary()+" ";
        }
		return ret;
	}
}
