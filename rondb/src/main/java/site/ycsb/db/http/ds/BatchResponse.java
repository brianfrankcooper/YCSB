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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Batch response.
 */
public class BatchResponse {
  private String bodyStr;
  private Map<String, PKResponse> subResponses = new HashMap<>();

  public BatchResponse(String body) {
    this.bodyStr = body;
    parse();
  }

  public void parse() {
    JsonObject jsonObj = JsonParser.parseString(bodyStr).getAsJsonObject();
    JsonArray jsonArray = jsonObj.get("result").getAsJsonArray();
    Iterator<JsonElement> itr = jsonArray.iterator();

    while (itr.hasNext()) {
      JsonObject body = itr.next().getAsJsonObject().get("body").getAsJsonObject();
      PKResponse pkResponse = new PKResponse(body);
      subResponses.put(pkResponse.getOpId(), pkResponse);
    }
  }

  public Collection<PKResponse> getSubResponses() {
    return subResponses.values();
  }

  public PKResponse getSubResponses(String id) {
    return subResponses.get(id);
  }
}
