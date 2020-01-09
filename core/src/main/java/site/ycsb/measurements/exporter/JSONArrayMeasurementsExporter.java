/**
 * Copyright (c) 2015-2016 Yahoo! Inc., 2017 YCSB contributors All rights reserved.
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
package site.ycsb.measurements.exporter;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.util.DefaultPrettyPrinter;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

/**
 * Export measurements into a machine readable JSON Array of measurement objects.
 */
public class JSONArrayMeasurementsExporter implements MeasurementsExporter {
  private final JsonFactory factory = new JsonFactory();
  private JsonGenerator g;

  public JSONArrayMeasurementsExporter(OutputStream os) throws IOException {
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os));
    g = factory.createJsonGenerator(bw);
    g.setPrettyPrinter(new DefaultPrettyPrinter());
    g.writeStartArray();
  }

  public void write(String metric, String measurement, int i) throws IOException {
    g.writeStartObject();
    g.writeStringField("metric", metric);
    g.writeStringField("measurement", measurement);
    g.writeNumberField("value", i);
    g.writeEndObject();
  }

  public void write(String metric, String measurement, long i) throws IOException {
    g.writeStartObject();
    g.writeStringField("metric", metric);
    g.writeStringField("measurement", measurement);
    g.writeNumberField("value", i);
    g.writeEndObject();
  }

  public void write(String metric, String measurement, double d) throws IOException {
    g.writeStartObject();
    g.writeStringField("metric", metric);
    g.writeStringField("measurement", measurement);
    g.writeNumberField("value", d);
    g.writeEndObject();
  }

  public void close() throws IOException {
    if (g != null) {
      g.writeEndArray();
      g.close();
    }
  }
}
