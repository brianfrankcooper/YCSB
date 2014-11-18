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
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;

import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;


/**
 * Take measurements and maintain a histogram of a given metric, such as READ LATENCY.
 *
 * @author cooperb
 */
public class OneMeasurementHistogram extends OneMeasurement {
    public static final String BUCKETS = "histogram.buckets";
    public static final String BUCKETS_DEFAULT = "1000";

    private final AtomicInteger buckets;
    private final AtomicIntegerArray histogram;
    private final AtomicInteger histogramoverflow = new AtomicInteger(0);
    private final AtomicInteger operations = new AtomicInteger(0);
    private final AtomicInteger retrycounts = new AtomicInteger(0);
    private final AtomicLong totallatency = new AtomicLong(0);

    //keep a windowed version of these stats for printing status
    private final AtomicInteger windowoperations = new AtomicInteger(0);
    private final AtomicLong windowtotallatency = new AtomicLong(0);

    private final AtomicInteger min = new AtomicInteger(-1);
    private final AtomicInteger max = new AtomicInteger(-1);
    private final ConcurrentMap<Integer, AtomicInteger> returncodes = new ConcurrentHashMap<Integer, AtomicInteger>();

    public OneMeasurementHistogram(String name, Properties props) {
        super(name);
        buckets = new AtomicInteger(Integer.parseInt(props.getProperty(BUCKETS, BUCKETS_DEFAULT)));
        histogram = new AtomicIntegerArray(buckets.get());
    }

    /* (non-Javadoc)
      * @see com.yahoo.ycsb.OneMeasurement#reportReturnCode(int)
      */
    public void reportReturnCode(int code) {
        AtomicInteger count = returncodes.get(code);
        if (count == null) {
            count = new AtomicInteger();
            AtomicInteger oldCount = returncodes.putIfAbsent(code, count);
            if (oldCount != null) {
                count = oldCount;
            }
        }
        count.incrementAndGet();
    }

    @Override
    public void reportRetryCount(int count) {
        retrycounts.addAndGet(count);
    }


    /* (non-Javadoc)
      * @see com.yahoo.ycsb.OneMeasurement#measure(int)
      */
    public void measure(int latency) {
        if (latency / 1000 >= buckets.get()) {
            histogramoverflow.incrementAndGet();
        } else {
            histogram.incrementAndGet(latency / 1000);
        }
        operations.incrementAndGet();
        totallatency.addAndGet(latency);
        windowoperations.incrementAndGet();
        windowtotallatency.addAndGet(latency);

        int lastMin = min.get();
        while ((lastMin < 0) || (latency < lastMin)) {
            if (min.compareAndSet(lastMin, latency)) {
                break;
            }
            lastMin = min.get();
        }

        int lastMax = max.get();
        while (latency > lastMax) {
            if (max.compareAndSet(lastMax, latency)) {
                break;
            }
            lastMax = max.get();
        }
    }


    @Override
    public void exportMeasurements(MeasurementsExporter exporter) throws IOException {
        exportGeneralMeasurements(exporter);
        exportMeasurementsPart(exporter);
    }

    @Override
    public void exportMeasurementsPart(MeasurementsExporter exporter) throws IOException {
        //do nothing for this type of measurements
    }

    private void exportGeneralMeasurements(MeasurementsExporter exporter) throws IOException {
        exporter.write(getName(), "Operations", operations.get());
        exporter.write(getName(), "Retries", retrycounts.get());
        exporter.write(getName(), "AverageLatency(us)", (((double) totallatency.get()) / ((double) operations.get())));
        exporter.write(getName(), "MinLatency(us)", min.get());
        exporter.write(getName(), "MaxLatency(us)", max.get());

        int opcounter = 0;
        boolean done95th = false;
        for (int i = 0; i < buckets.get(); i++) {
            opcounter += histogram.get(i);
            if ((!done95th) && (((double) opcounter) / ((double) operations.get()) >= 0.95)) {
                exporter.write(getName(), "95thPercentileLatency(ms)", i);
                done95th = true;
            }
            if (((double) opcounter) / ((double) operations.get()) >= 0.99) {
                exporter.write(getName(), "99thPercentileLatency(ms)", i);
                break;
            }
        }

        for (Integer I : returncodes.keySet()) {
            exporter.write(getName(), "Return=" + I, returncodes.get(I).get());
        }
    }

    @Override
    public void exportMeasurementsFinal(MeasurementsExporter exporter) throws IOException {
        for (int i = 0; i < buckets.get(); i++) {
            int count = histogram.get(i);
            if (count > 0)
                exporter.write(getName(), Integer.toString(i), count);
        }
        if (histogramoverflow.get() > 0)
            exporter.write(getName(), ">" + buckets.get(), histogramoverflow.get());
        exportGeneralMeasurements(exporter);
    }

    @Override
    public String getSummary() {
        if (windowoperations.get() == 0) {
            return "";
        }
        DecimalFormat d = new DecimalFormat("#.##");
        double report = ((double) windowtotallatency.get()) / ((double) windowoperations.get());
        windowtotallatency.set(0);
        windowoperations.set(0);
        return "[" + getName() + " AverageLatency(us)=" + d.format(report) + "]";
    }

}
