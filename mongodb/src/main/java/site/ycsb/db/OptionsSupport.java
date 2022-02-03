/*
 * Copyright (c) 2014, Yahoo!, Inc. All rights reserved.
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
package site.ycsb.db;

import java.util.Properties;

/**
 * OptionsSupport provides methods for handling legacy options.
 *
 * @author rjm
 */
public final class OptionsSupport {

  /** Value for an unavailable property. */
  private static final String UNAVAILABLE = "n/a";

  /**
   * Updates the URL with the appropriate attributes if legacy properties are
   * set and the URL does not have the property already set.
   *
   * @param url
   *          The URL to update.
   * @param props
   *          The legacy properties.
   * @return The updated URL.
   */
  public static String updateUrl(String url, Properties props) {
    String result = url;

    // max connections.
    final String maxConnections =
        props.getProperty("mongodb.maxconnections", UNAVAILABLE).toLowerCase();
    if (!UNAVAILABLE.equals(maxConnections)) {
      result = addUrlOption(result, "maxPoolSize", maxConnections);
    }

    // Blocked thread multiplier.
    final String threadsAllowedToBlockForConnectionMultiplier =
        props
            .getProperty(
                "mongodb.threadsAllowedToBlockForConnectionMultiplier",
                UNAVAILABLE).toLowerCase();
    if (!UNAVAILABLE.equals(threadsAllowedToBlockForConnectionMultiplier)) {
      result =
          addUrlOption(result, "waitQueueMultiple",
              threadsAllowedToBlockForConnectionMultiplier);
    }

    // write concern
    String writeConcernType =
        props.getProperty("mongodb.writeConcern", UNAVAILABLE).toLowerCase();
    if (!UNAVAILABLE.equals(writeConcernType)) {
      if ("errors_ignored".equals(writeConcernType)) {
        result = addUrlOption(result, "w", "0");
      } else if ("unacknowledged".equals(writeConcernType)) {
        result = addUrlOption(result, "w", "0");
      } else if ("acknowledged".equals(writeConcernType)) {
        result = addUrlOption(result, "w", "1");
      } else if ("journaled".equals(writeConcernType)) {
        result = addUrlOption(result, "journal", "true"); // this is the
        // documented option
        // name
        result = addUrlOption(result, "j", "true"); // but keep this until
        // MongoDB Java driver
        // supports "journal" option
      } else if ("replica_acknowledged".equals(writeConcernType)) {
        result = addUrlOption(result, "w", "2");
      } else if ("majority".equals(writeConcernType)) {
        result = addUrlOption(result, "w", "majority");
      } else {
        System.err.println("WARNING: Invalid writeConcern: '"
            + writeConcernType + "' will be ignored. "
            + "Must be one of [ unacknowledged | acknowledged | "
            + "journaled | replica_acknowledged | majority ]");
      }
    }

    // read preference
    String readPreferenceType =
        props.getProperty("mongodb.readPreference", UNAVAILABLE).toLowerCase();
    if (!UNAVAILABLE.equals(readPreferenceType)) {
      if ("primary".equals(readPreferenceType)) {
        result = addUrlOption(result, "readPreference", "primary");
      } else if ("primary_preferred".equals(readPreferenceType)) {
        result = addUrlOption(result, "readPreference", "primaryPreferred");
      } else if ("secondary".equals(readPreferenceType)) {
        result = addUrlOption(result, "readPreference", "secondary");
      } else if ("secondary_preferred".equals(readPreferenceType)) {
        result = addUrlOption(result, "readPreference", "secondaryPreferred");
      } else if ("nearest".equals(readPreferenceType)) {
        result = addUrlOption(result, "readPreference", "nearest");
      } else {
        System.err.println("WARNING: Invalid readPreference: '"
            + readPreferenceType + "' will be ignored. "
            + "Must be one of [ primary | primary_preferred | "
            + "secondary | secondary_preferred | nearest ]");
      }
    }

    return result;
  }

  /**
   * Adds an option to the url if it does not already contain the option.
   *
   * @param url
   *          The URL to append the options to.
   * @param name
   *          The name of the option.
   * @param value
   *          The value for the option.
   * @return The updated URL.
   */
  private static String addUrlOption(String url, String name, String value) {
    String fullName = name + "=";
    if (!url.contains(fullName)) {
      if (url.contains("?")) {
        return url + "&" + fullName + value;
      }
      return url + "?" + fullName + value;
    }
    return url;
  }

  /**
   * Hidden Constructor.
   */
  private OptionsSupport() {
    // Nothing.
  }
}
