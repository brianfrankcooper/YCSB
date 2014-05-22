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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;

class SeriesUnit {
    /**
     * @param time
     * @param average
     */
    public SeriesUnit(long time, double average, double throughput) {
        this.time = time;
        this.average = average;
        this.throughput = throughput;
    }

    public long time;
    public double average;
    public double throughput;
}

/**
 * A time series measurement of a metric, such as READ LATENCY.
 */
public class OneMeasurementTimeSeries extends OneMeasurement {
    /**
     * Granularity for time series; measurements will be averaged in chunks of this granularity. Units are milliseconds.
     */
    public static final String GRANULARITY = "timeseries.granularity";
    public static final String GRANULARITY_DEFAULT = "1000";

    int _granularity;
    private Vector<SeriesUnit> _measurements;

    private AtomicLong start = new AtomicLong(-1);
    private AtomicLong currentunit = new AtomicLong(-1);
    private AtomicInteger count = new AtomicInteger(0);
    private AtomicInteger sum = new AtomicInteger(0);
    private AtomicInteger operations = new AtomicInteger(0);
    private AtomicLong totallatency = new AtomicLong(0);
    private AtomicInteger retrycounts = new AtomicInteger(0);

    //keep a windowed version of these stats for printing status
    private AtomicInteger windowoperations = new AtomicInteger(0);
    private AtomicLong windowtotallatency = new AtomicLong(0);

    private AtomicInteger min = new AtomicInteger(-1);
    private AtomicInteger max = new AtomicInteger(-1);

    private int first = 0;

    private final ConcurrentMap<Integer, AtomicInteger> returncodes = new ConcurrentHashMap<Integer, AtomicInteger>();

    public OneMeasurementTimeSeries(String name, Properties props) {
        super(name);
        _granularity = Integer.parseInt(props.getProperty(GRANULARITY, GRANULARITY_DEFAULT));
        _measurements = new Vector<SeriesUnit>();
    }

    void checkEndOfUnit(boolean forceend) {
        long now = System.currentTimeMillis();

        if (start.get() < 0) {
            currentunit.set(0);
            start.set(now);
        }

        long unit = ((now - start.get()) / _granularity) * _granularity;

        if ((unit > currentunit.get()) || (forceend)) {
            double avg = ((double) sum.get()) / ((double) count.get());
            _measurements.add(new SeriesUnit(currentunit.get(), avg, count.get() / (_granularity / 1000.0)));

            currentunit.set(unit);

            count.set(0);
            sum.set(0);
        }
    }

    @Override
    public void measure(int latency) {
        checkEndOfUnit(false);

        count.incrementAndGet();
        sum.addAndGet(latency);
        totallatency.addAndGet(latency);
        operations.incrementAndGet();
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
        checkEndOfUnit(true);
        exportGeneralMeasurements(exporter);

        for (SeriesUnit unit : _measurements) {
            exporter.write(getName(), Long.toString(unit.time), unit.average, unit.throughput);
        }
    }

    private void exportGeneralMeasurements(MeasurementsExporter exporter) throws IOException {
        exporter.write(getName(), "Operations", operations.get());
        exporter.write(getName(), "Retries", retrycounts.get());
        exporter.write(getName(), "AverageLatency(us)", (((double) totallatency.get()) / ((double) operations.get())));
        exporter.write(getName(), "MinLatency(us)", min.get());
        exporter.write(getName(), "MaxLatency(us)", max.get());

        //TODO: 95th and 99th percentile latency

        for (Integer I : returncodes.keySet()) {
            exporter.write(getName(), "Return=" + I, returncodes.get(I).get());
        }
    }

    @Override
    public void exportMeasurementsPart(MeasurementsExporter exporter) throws IOException {
        int last = _measurements.size();
        for (int i = first; i < last; i++) {
            SeriesUnit unit = _measurements.get(i);
            exporter.write(getName(), Long.toString(unit.time), unit.average, unit.throughput);
        }
        first = last;
    }

    @Override
    public void exportMeasurementsFinal(MeasurementsExporter exporter) throws IOException {
        checkEndOfUnit(true);
        exportMeasurementsPart(exporter);
        exportGeneralMeasurements(exporter);
    }

    @Override
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
