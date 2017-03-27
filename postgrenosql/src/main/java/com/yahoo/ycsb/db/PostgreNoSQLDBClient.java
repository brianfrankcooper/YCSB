/*
  Copyright (c) 2012 - 2015 YCSB contributors. All rights reserved.

  Licensed under the Apache License, Version 2.0 (the "License"); you
  may not use this file except in compliance with the License. You
  may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
  implied. See the License for the specific language governing
  permissions and limitations under the License. See accompanying
  LICENSE file.
 */
package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Status;

import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

/**
 * PostgreNoSQL client for YCSB framework.
 */
public class PostgreNoSQLDBClient extends DB {

  @Override
  public Status read(String tableName, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    return Status.OK;
  }

  @Override
  public Status scan(String tableName, String startKey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    return Status.OK;
  }

  @Override
  public Status update(String tableName, String key, HashMap<String, ByteIterator> values) {
    return Status.OK;
  }

  @Override
  public Status insert(String tableName, String key, HashMap<String, ByteIterator> values) {
    return Status.OK;
  }

  @Override
  public Status delete(String tableName, String key) {
    return Status.OK;
  }
}






