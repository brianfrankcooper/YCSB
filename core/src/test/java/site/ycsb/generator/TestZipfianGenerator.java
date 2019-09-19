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

package site.ycsb.generator;

import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertFalse;


public class TestZipfianGenerator {
    @Test
    public void testMinAndMaxParameter() {
        long min = 5;
        long max = 10;
        ZipfianGenerator zipfian = new ZipfianGenerator(min, max);

        for (int i = 0; i < 10000; i++) {
            long rnd = zipfian.nextValue();
            assertFalse(rnd < min);
            assertFalse(rnd > max);
        }

    }
}
