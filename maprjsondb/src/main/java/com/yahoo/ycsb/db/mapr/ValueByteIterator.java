/*
 * Copyright (c) 2017, Yahoo!, Inc. All rights reserved.
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

package com.yahoo.ycsb.db.mapr;

import org.ojai.Value;
import org.ojai.util.Values;

import com.yahoo.ycsb.ByteIterator;

/**
 * OJAI Value byte iterator.
 * 
 * Used for parsing the document fetched MapR JSON DB
 */
public class ValueByteIterator extends ByteIterator {

  private Value value;

  public ValueByteIterator(Value value) {
    this.value = value;
  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public byte nextByte() {
    return 0;
  }

  @Override
  public long bytesLeft() {
    return 0;
  }

  @Override
  public String toString() {
    return Values.asJsonString(value);
  }

}
