/*
 * Copyright (c) 2022, Yahoo!, Inc. All rights reserved.
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

/**
 * Batch sub op.
 */
public class BatchSubOperation {
  private String method = "POST";
  private String relativeURL;
  private PKRequest pkRequest;
  public BatchSubOperation(String relativeURL, PKRequest pkReq){
    this.relativeURL = relativeURL;
    this.pkRequest = pkReq;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append("\"method\":");
    sb.append("\"");
    sb.append(method);
    sb.append("\"");
    sb.append(",");
    sb.append("\"relative-url\":");
    sb.append("\"");
    sb.append(relativeURL);
    sb.append("\"");
    sb.append(",");
    sb.append("\"body\":");
    sb.append(pkRequest.toString());
    sb.append("}");
    return sb.toString();
  }
}
