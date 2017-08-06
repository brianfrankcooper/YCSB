/**
 * Copyright (c) 2017 YCSB contributors. All rights reserved.
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
package com.yahoo.ycsb;

import org.testng.annotations.Test;
import static org.testng.AssertJUnit.*;

public class TestNumericByteIterator {

  @Test
  public void testLong() throws Exception {
    NumericByteIterator it = new NumericByteIterator(42L);
    assertFalse(it.isFloatingPoint());
    assertEquals(42L, it.getLong());
    
    try {
      it.getDouble();
      fail("Expected IllegalStateException.");
    } catch (IllegalStateException e) { }
    try {
      it.next();
      fail("Expected UnsupportedOperationException.");
    } catch (UnsupportedOperationException e) { }
    
    assertEquals(8, it.bytesLeft());
    assertTrue(it.hasNext());
    assertEquals((byte) 0, (byte) it.nextByte());
    assertEquals(7, it.bytesLeft());
    assertTrue(it.hasNext());
    assertEquals((byte) 0, (byte) it.nextByte());
    assertEquals(6, it.bytesLeft());
    assertTrue(it.hasNext());
    assertEquals((byte) 0, (byte) it.nextByte());
    assertEquals(5, it.bytesLeft());
    assertTrue(it.hasNext());
    assertEquals((byte) 0, (byte) it.nextByte());
    assertEquals(4, it.bytesLeft());
    assertTrue(it.hasNext());
    assertEquals((byte) 0, (byte) it.nextByte());
    assertEquals(3, it.bytesLeft());
    assertTrue(it.hasNext());
    assertEquals((byte) 0, (byte) it.nextByte());
    assertEquals(2, it.bytesLeft());
    assertTrue(it.hasNext());
    assertEquals((byte) 0, (byte) it.nextByte());
    assertEquals(1, it.bytesLeft());
    assertTrue(it.hasNext());
    assertEquals((byte) 42, (byte) it.nextByte());
    assertEquals(0, it.bytesLeft());
    assertFalse(it.hasNext());
    
    it.reset();
    assertTrue(it.hasNext());
    assertEquals((byte) 0, (byte) it.nextByte());
  }
  
  @Test
  public void testDouble() throws Exception {
    NumericByteIterator it = new NumericByteIterator(42.75);
    assertTrue(it.isFloatingPoint());
    assertEquals(42.75, it.getDouble(), 0.001);
    
    try {
      it.getLong();
      fail("Expected IllegalStateException.");
    } catch (IllegalStateException e) { }
    try {
      it.next();
      fail("Expected UnsupportedOperationException.");
    } catch (UnsupportedOperationException e) { }
    
    assertEquals(8, it.bytesLeft());
    assertTrue(it.hasNext());
    assertEquals((byte) 64, (byte) it.nextByte());
    assertEquals(7, it.bytesLeft());
    assertTrue(it.hasNext());
    assertEquals((byte) 69, (byte) it.nextByte());
    assertEquals(6, it.bytesLeft());
    assertTrue(it.hasNext());
    assertEquals((byte) 96, (byte) it.nextByte());
    assertEquals(5, it.bytesLeft());
    assertTrue(it.hasNext());
    assertEquals((byte) 0, (byte) it.nextByte());
    assertEquals(4, it.bytesLeft());
    assertTrue(it.hasNext());
    assertEquals((byte) 0, (byte) it.nextByte());
    assertEquals(3, it.bytesLeft());
    assertTrue(it.hasNext());
    assertEquals((byte) 0, (byte) it.nextByte());
    assertEquals(2, it.bytesLeft());
    assertTrue(it.hasNext());
    assertEquals((byte) 0, (byte) it.nextByte());
    assertEquals(1, it.bytesLeft());
    assertTrue(it.hasNext());
    assertEquals((byte) 0, (byte) it.nextByte());
    assertEquals(0, it.bytesLeft());
    assertFalse(it.hasNext());
    
    it.reset();
    assertTrue(it.hasNext());
    assertEquals((byte) 64, (byte) it.nextByte());
  }
}
