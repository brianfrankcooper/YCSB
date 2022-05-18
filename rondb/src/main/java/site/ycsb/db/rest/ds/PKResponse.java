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
package site.ycsb.db.rest.ds;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * PKResponse.
 */
public class PKResponse {
  private String bodyStr = null;
  private JsonObject bodyJsonObj = null;
  private Map<Integer, Map<String, String>> bodyMap = new HashMap<>();
  private int opId;

  public PKResponse(String body) {
    this.bodyStr = body;
    parseStr();
  }

  public PKResponse(JsonObject bodyJsonObj) {
    this.bodyJsonObj = bodyJsonObj;
    parseJsonObj();
  }

  private void parseStr() {
    JsonParser parser = new JsonParser();
    bodyJsonObj = parser.parse(bodyStr).getAsJsonObject();
    parseJsonObj();
  }

  private void parseJsonObj() {
    Map<String, String> data = new HashMap<>();
    opId = bodyJsonObj.get("operationId").getAsInt();
    Set<Map.Entry<String, JsonElement>> eset = bodyJsonObj.get("data").getAsJsonObject().entrySet();
    for (Map.Entry<String, JsonElement> e : eset) {
      data.put(e.getKey(), e.getValue().getAsString());
    }
    bodyMap.put(opId, data);
  }

  public String getData(int opID, String filed) {
    return bodyMap.get(opID).get(filed);
  }

  public int getOpId() {
    return opId;
  }
}
