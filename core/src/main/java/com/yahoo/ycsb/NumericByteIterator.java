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

/**
 * A byte iterator that handles encoding and decoding numeric values.
 * Currently this iterator can handle 64 bit signed values and double precision
 * floating point values.
 */
public class NumericByteIterator extends ByteIterator {
  private final byte[] payload;
  private final boolean floatingPoint;
  private int off;
  
  public NumericByteIterator(final long value) {
    floatingPoint = false;
    payload = Utils.longToBytes(value);
    off = 0;
  }
  
  public NumericByteIterator(final double value) {
    floatingPoint = true;
    payload = Utils.doubleToBytes(value);
    off = 0;
  }
  
  @Override
  public boolean hasNext() {
    return off < payload.length;
  }

  @Override
  public byte nextByte() {
    return payload[off++];
  }

  @Override
  public long bytesLeft() {
    return payload.length - off;
  }

  @Override
  public void reset() {
    off = 0;
  }
  
  public long getLong() {
    if (floatingPoint) {
      throw new IllegalStateException("Byte iterator is of the type double");
    }
    return Utils.bytesToLong(payload);
  }

  public double getDouble() {
    if (!floatingPoint) {
      throw new IllegalStateException("Byte iterator is of the type long");
    }
    return Utils.bytesToDouble(payload);
  }

  public boolean isFloatingPoint() {
    return floatingPoint;
  }

}