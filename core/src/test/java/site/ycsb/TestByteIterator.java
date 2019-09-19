/**
 * Copyright (c) 2012 YCSB contributors. All rights reserved.
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

package site.ycsb;

import org.testng.annotations.Test;
import static org.testng.AssertJUnit.*;

public class TestByteIterator {
  @Test
  public void testRandomByteIterator() {
    int size = 100;
    ByteIterator itor = new RandomByteIterator(size);
    assertTrue(itor.hasNext());
    assertEquals(size, itor.bytesLeft());
    assertEquals(size, itor.toString().getBytes().length);
    assertFalse(itor.hasNext());
    assertEquals(0, itor.bytesLeft());

    itor = new RandomByteIterator(size);
    assertEquals(size, itor.toArray().length);
    assertFalse(itor.hasNext());
    assertEquals(0, itor.bytesLeft());
  }
}
