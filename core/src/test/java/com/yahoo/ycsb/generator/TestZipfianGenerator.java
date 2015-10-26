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

package com.yahoo.ycsb.generator;

import static org.testng.AssertJUnit.assertFalse;

import org.testng.annotations.Test;

public class TestZipfianGenerator {
  @Test
  public void testMinAndMaxParameter() {
    final long min = 5;
    final long max = 10;
    final ZipfianGenerator zipfian = new ZipfianGenerator(min, max);

    for (int i = 0; i < 10000; i++) {
      final long rnd = zipfian.nextLong();
      assertFalse(rnd < min);
      assertFalse(rnd > max);
    }

  }
}
