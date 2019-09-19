/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors All rights reserved.
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
package site.ycsb;

/**
 *  A ByteIterator that iterates through a byte array.
 */
public class ByteArrayByteIterator extends ByteIterator {
  private final int originalOffset;
  private final byte[] str;
  private int off;
  private final int len;

  public ByteArrayByteIterator(byte[] s) {
    this.str = s;
    this.off = 0;
    this.len = s.length;
    originalOffset = 0;
  }

  public ByteArrayByteIterator(byte[] s, int off, int len) {
    this.str = s;
    this.off = off;
    this.len = off + len;
    originalOffset = off;
  }

  @Override
  public boolean hasNext() {
    return off < len;
  }

  @Override
  public byte nextByte() {
    byte ret = str[off];
    off++;
    return ret;
  }

  @Override
  public long bytesLeft() {
    return len - off;
  }

  @Override
  public void reset() {
    off = originalOffset;
  }

  @Override
  public byte[] toArray() {
    int size = (int) bytesLeft();
    byte[] bytes = new byte[size];
    System.arraycopy(str, off, bytes, 0, size);
    off = len;
    return bytes;
  }

}
