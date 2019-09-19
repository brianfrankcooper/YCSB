/*
 * Copyright (c) 2017 YCSB contributors. All rights reserved.
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

package site.ycsb.db.elasticsearch5;

import java.util.Properties;

final class Elasticsearch5 {

  private Elasticsearch5() {

  }

  static final String KEY = "key";

  static int parseIntegerProperty(final Properties properties, final String key, final int defaultValue) {
    final String value = properties.getProperty(key);
    return value == null ? defaultValue : Integer.parseInt(value);
  }

}
