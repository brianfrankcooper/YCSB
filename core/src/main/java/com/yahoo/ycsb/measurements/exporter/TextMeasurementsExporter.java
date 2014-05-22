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
package com.yahoo.ycsb.measurements.exporter;

import java.io.*;

/**
 * Write human readable text. Tries to emulate the previous print report method.
 */
public class TextMeasurementsExporter implements MeasurementsExporter {

    private PrintStream ps;

    public TextMeasurementsExporter(OutputStream os) {
        this.ps = new PrintStream(os);
    }

    public void write(String metric, String measurement, int i) throws IOException {
        ps.println("[" + metric + "], " + measurement + ", " + i);
    }

    public void write(String metric, String measurement, double d) throws IOException {
        ps.println("[" + metric + "], " + measurement + ", " + d);
    }

    @Override
    public void write(String metric, String measurement, double i, double t) throws IOException {
        ps.println("[" + metric + "], " + measurement + ", " + i + ", " + t);
    }

    public void close() throws IOException {
        this.ps.close();
    }
}
