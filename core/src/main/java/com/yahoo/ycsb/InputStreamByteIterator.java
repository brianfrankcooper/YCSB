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
package com.yahoo.ycsb;

import java.io.InputStream;

/**
 *  A ByteIterator that iterates through an inputstream of bytes.
 */
public class InputStreamByteIterator extends ByteIterator {
  private long len;
  private InputStream ins;
  private long off;

  public InputStreamByteIterator(InputStream ins, long len) {
    this.len = len;
    this.ins = ins;
    off = 0;
  }

  @Override
  public boolean hasNext() {
    return off < len;
  }

  @Override
  public byte nextByte() {
    int ret;
    try {
      ret = ins.read();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    if (ret == -1) {
      throw new IllegalStateException("Past EOF!");
    }
    off++;
    return (byte) ret;
  }

  @Override
  public long bytesLeft() {
    return len - off;
  }

}
