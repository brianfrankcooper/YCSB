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
import java.text.DecimalFormat;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;

/**
 * Take measurements and maintain a histogram of a given metric, such as READ LATENCY.
 * 
 * @author cooperb
 *
 */
public class OneMeasurementHdrHistogram extends OneMeasurement {

    Recorder histogram = new Recorder(3);
    
    final ConcurrentHashMap<Integer, AtomicInteger> returncodes;

    Histogram totalHistogram;

    public OneMeasurementHdrHistogram(String name, Properties props) {
        super(name);
        returncodes = new ConcurrentHashMap<Integer, AtomicInteger>();
    }

    /**
     * No need for synchronization, using CHM to deal with that
     * 
     * @see com.yahoo.ycsb.OneMeasurement#reportReturnCode(int)
     */
    public void reportReturnCode(int code) {
        Integer Icode = code;
        AtomicInteger counter = returncodes.get(Icode);
        if (counter == null) {
            AtomicInteger other = returncodes.putIfAbsent(Icode, counter = new AtomicInteger());
            if (other != null) {
                counter = other;
            }
        }

        counter.incrementAndGet();
    }

    /**
     * It appears latency is reported in micros.
     * Using {@link ConcurrentHistogram} to support concurrent updates to histogram.
     * 
     * @see com.yahoo.ycsb.OneMeasurement#measure(int)
     */
    public void measure(int latencyInMicros) {
        histogram.recordValue(latencyInMicros);
    }

    /**
     * This is called from a main thread, on orderly termination.
     * 
     * @see com.yahoo.ycsb.measurements.OneMeasurement#exportMeasurements(com.yahoo.ycsb.measurements.exporter.MeasurementsExporter)
     */
    @Override
    public void exportMeasurements(MeasurementsExporter exporter) throws IOException {
        Histogram lastIntervalHistogram = histogram.getIntervalHistogram();
        // add this to the total time histogram.
        if (totalHistogram == null) {
            totalHistogram = lastIntervalHistogram; 
        }
        else {
            totalHistogram.add(lastIntervalHistogram);
        }
        exporter.write(getName(), "Operations", totalHistogram.getTotalCount());
        exporter.write(getName(), "AverageLatency(us)", totalHistogram.getMean());
        exporter.write(getName(), "MinLatency(us)", totalHistogram.getMinValue());
        exporter.write(getName(), "MaxLatency(us)", totalHistogram.getMaxValue());
        exporter.write(getName(), "95thPercentileLatency(ms)", totalHistogram.getValueAtPercentile(90)/1000);
        exporter.write(getName(), "99thPercentileLatency(ms)", totalHistogram.getValueAtPercentile(99)/1000);

        for (Map.Entry<Integer, AtomicInteger> entry : returncodes.entrySet()) {
            exporter.write(getName(), "Return=" + entry.getKey(), entry.getValue().get());
        }

    }

    /**
     * This is called periodically from the StatusThread. There's a single StatusThread per Client process.
     * @see com.yahoo.ycsb.measurements.OneMeasurement#getSummary()
     */
    @Override
    public String getSummary() {
        Histogram intervalHistogram = histogram.getIntervalHistogram();
        // add this to the total time histogram.
        if (totalHistogram == null) {
            totalHistogram = intervalHistogram; 
        }
        else {
            totalHistogram.add(intervalHistogram);
        }
        DecimalFormat d = new DecimalFormat("#.##");
        return "[" + getName() + 
                ": Count=" + intervalHistogram.getTotalCount() + 
                ", Max=" + intervalHistogram.getMaxValue() + 
                ", Min=" + intervalHistogram.getMinValue() +
                ", Avg=" + d.format(intervalHistogram.getMean()) +
                ", 90=" + d.format(intervalHistogram.getValueAtPercentile(90)) +
                ", 99=" + d.format(intervalHistogram.getValueAtPercentile(99)) +
                ", 99.9=" + d.format(intervalHistogram.getValueAtPercentile(99.9)) +
                ", 99.99=" + d.format(intervalHistogram.getValueAtPercentile(99.99)) +"]";
    }

}
