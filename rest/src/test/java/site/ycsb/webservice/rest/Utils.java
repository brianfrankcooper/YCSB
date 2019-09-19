/**
 * Copyright (c) 2016 YCSB contributors. All rights reserved.
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

package site.ycsb.webservice.rest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;

/**
 * Holds the common utility methods.
 */
public class Utils {

  /**
   * Returns true if the port is available.
   * 
   * @param port
   * @return isAvailable
   */
  public static boolean available(int port) {
    ServerSocket ss = null;
    DatagramSocket ds = null;
    try {
      ss = new ServerSocket(port);
      ss.setReuseAddress(true);
      ds = new DatagramSocket(port);
      ds.setReuseAddress(true);
      return true;
    } catch (IOException e) {
    } finally {
      if (ds != null) {
        ds.close();
      }
      if (ss != null) {
        try {
          ss.close();
        } catch (IOException e) {
          /* should not be thrown */
        }
      }
    }
    return false;
  }

  public static List<String> read(String filepath) {
    List<String> list = new ArrayList<String>();
    try {
      BufferedReader file = new BufferedReader(new FileReader(filepath));
      String line = null;
      while ((line = file.readLine()) != null) {
        list.add(line.trim());
      }
      file.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return list;
  }

  public static void delete(String filepath) {
    try {
      new File(filepath).delete();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
