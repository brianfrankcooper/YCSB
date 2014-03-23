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

import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.apache.commons.math.stat.descriptive.SynchronizedSummaryStatistics;

/**
 * This class records and prints statistics of YCSB measurements using 
 * apache.commons.math classes. The output includes the mean value, 
 * the standard deviation and the 95% confidence interval assuming there are
 * more than 30 recorded values.
 * 
 * @author Christian Spann <christian dot spann at uni-ulm dot de>
 */
public class OneMeasurementStatistics extends OneMeasurement
{

	private final SynchronizedSummaryStatistics responsetimes = new SynchronizedSummaryStatistics();
	int operations;
	HashMap<Integer, int[]> returncodes;

	public OneMeasurementStatistics(String _name)
	{
		super(_name);
		returncodes=new HashMap<Integer,int[]>();
	}

	@Override
	public void reportReturnCode(int code)
	{
		Integer Icode = code;
		if (!returncodes.containsKey(Icode))
		{
			int[] val = new int[1];
			val[0] = 0;
			returncodes.put(Icode, val);
		}
		returncodes.get(Icode)[0]++;
	}

	@Override
	public void measure(int latency)
	{
		responsetimes.addValue(latency);
		operations++;
	}

	@Override
	public String getSummary()
	{
		DecimalFormat d = new DecimalFormat("#.##");
		return "["+getName()+" AverageLatency(us)="+d.format(responsetimes.getStandardDeviation())+"]";
	}

	@Override
	public void exportMeasurements(MeasurementsExporter exporter) throws IOException
	{
		exporter.write(getName(), "Operations", operations);
		exporter.write(getName(), "AverageLatency(us)", responsetimes.getMean());
		exporter.write(getName(), "StdDev(us)", responsetimes.getStandardDeviation());
		exporter.write(getName(), "95% Confidence Interval (us)", get95ConfidenceIntervalWidth(responsetimes));

		for (Integer I : returncodes.keySet())
		{
			int[] val = returncodes.get(I);
			exporter.write(getName(), "Return=" + I, val[0]);
		}
	}
	
	/**
	 * Calculates the 95% confidence interval. If less than 30 measurements where
	 * recorded, 0.0 is returned.
	 * 
	 * @param summaryStatistics The statistics object holding the recorded values
	 * @return The 95% confidence interval or 0 if summaryStatistics.getN() is
	 * less than 30 
	 */
	public static double get95ConfidenceIntervalWidth(SummaryStatistics summaryStatistics)
	{
		double a = 1.960; // 95% confidence interval width for standard deviation
		return a * summaryStatistics.getStandardDeviation() / Math.sqrt(summaryStatistics.getN());
	}
}
