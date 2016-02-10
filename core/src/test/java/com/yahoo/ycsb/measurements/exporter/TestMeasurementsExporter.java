/**
 * Copyright (c) 2015 Yahoo! Inc. All rights reserved.
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
package com.yahoo.ycsb.measurements.exporter;

import com.yahoo.ycsb.generator.ZipfianGenerator;
import com.yahoo.ycsb.measurements.Measurements;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

public class TestMeasurementsExporter {
    @Test
    public void testJSONArrayMeasurementsExporter() throws IOException {
        Properties props = new Properties();
        props.put(Measurements.MEASUREMENT_TYPE_PROPERTY, "histogram");
        Measurements mm = new Measurements(props);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        JSONArrayMeasurementsExporter export = new JSONArrayMeasurementsExporter(out);

        long min = 5000;
        long max = 100000;
        ZipfianGenerator zipfian = new ZipfianGenerator(min, max);
        for (int i = 0; i < 1000; i++) {
            int rnd = zipfian.nextValue().intValue();
            mm.measure("UPDATE", rnd);
        }
        mm.exportMeasurements(export);
        export.close();

        ObjectMapper mapper = new ObjectMapper();
        JsonNode  json = mapper.readTree(out.toString("UTF-8"));
        assertTrue(json.isArray());
        assertEquals(json.get(0).get("measurement").asText(), "Operations");
        assertEquals(json.get(4).get("measurement").asText(), "MaxLatency(us)");
        assertEquals(json.get(11).get("measurement").asText(), "4");
    }
}
