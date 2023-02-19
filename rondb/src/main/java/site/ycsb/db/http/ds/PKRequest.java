/*
 * Copyright (c) 2023, Hopsworks AB. All rights reserved.
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

/**
 * YCSB binding for <a href="https://rondb.com/">RonDB</a>.
 */
package site.ycsb.db.http.ds;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * PK Request.
 */
public class PKRequest {

  private String opID;
  private List<Filter> filters = new ArrayList<>();
  private List<ReadColumn> readColumns = new ArrayList<>();

  public PKRequest(String opID) {
    this.opID = opID;
  }

  public void addFilter(String colName, String value) {
    filters.add(new Filter(colName, value));
  }

  public void addReadColumn(String colName) {
    readColumns.add(new ReadColumn(colName));
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append("\"operationId\":");
    sb.append("\"");
    sb.append(opID);
    sb.append("\"");
    sb.append(",");
    sb.append("\"filters\":");
    sb.append(Arrays.toString(filters.toArray()));
    sb.append(",");
    sb.append("\"readColumns\":");
    sb.append(Arrays.toString(readColumns.toArray()));
    sb.append("}");
    return sb.toString();
  }

}
