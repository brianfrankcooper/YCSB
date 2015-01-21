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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.text.DecimalFormat;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;
import org.HdrHistogram.Recorder;

import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;

/**
 * Take measurements and maintain a HdrHistogram of a given metric, such as READ LATENCY.
 * 
 * @author nitsanw
 *
 */
public class OneMeasurementHdrHistogram extends OneMeasurement {
    // we need one log per measurement histogram
    final PrintStream log;
    final HistogramLogWriter histogramLogWriter;
    
    final Recorder histogram = new Recorder(3);
    final ConcurrentHashMap<Integer, AtomicInteger> returncodes;

    Histogram totalHistogram;

    public OneMeasurementHdrHistogram(String name, Properties props) {
        super(name);
        returncodes = new ConcurrentHashMap<Integer, AtomicInteger>();
        boolean shouldLog = Boolean.parseBoolean(props.getProperty("hdrhistogram.fileoutput", "false"));
        if (!shouldLog) {
            log = null;
            histogramLogWriter = null;
            return;
        }
        try {
            final String hdrOutputFilename = props.getProperty("hdrhistogram.output.path", "") +name+".hdr";
            log = new PrintStream(new FileOutputStream(hdrOutputFilename), false);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Failed to open hdr histogram output file",e);
        }
        histogramLogWriter = new HistogramLogWriter(log);
        histogramLogWriter.outputComment("[Logging for: " + name + "]");
        histogramLogWriter.outputLogFormatVersion();
        histogramLogWriter.outputStartTime(System.currentTimeMillis());
        histogramLogWriter.outputLegend();
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
     * Using {@link Recorder} to support concurrent updates to histogram.
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
        // accumulate the last interval which was not caught by status thread
        Histogram intervalHistogram = getIntervalHistogramAndAccumulate();
        if(histogramLogWriter != null) {
            histogramLogWriter.outputIntervalHistogram(intervalHistogram);
            // we can close now
            log.close();
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
     * We optionally serialize the interval to log on this opportunity.
     * @see com.yahoo.ycsb.measurements.OneMeasurement#getSummary()
     */
    @Override
    public String getSummary() {
        Histogram intervalHistogram = getIntervalHistogramAndAccumulate();
        // we use the summary interval as the histogram file interval.
        if(histogramLogWriter != null) {
            histogramLogWriter.outputIntervalHistogram(intervalHistogram);
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

    private Histogram getIntervalHistogramAndAccumulate() {
        Histogram intervalHistogram = histogram.getIntervalHistogram();
        // add this to the total time histogram.
        if (totalHistogram == null) {
            totalHistogram = intervalHistogram; 
        }
        else {
            totalHistogram.add(intervalHistogram);
        }
        return intervalHistogram;
    }

}
