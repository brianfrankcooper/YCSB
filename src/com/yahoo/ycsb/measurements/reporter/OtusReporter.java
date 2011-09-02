/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.yahoo.ycsb.measurements.reporter;

import java.io.*;
import java.net.*;
import java.net.InetAddress;
import java.util.Properties;

public class OtusReporter extends Reporter {

    private InetAddress address;
    private int port;
    private String host;
    private DatagramSocket socket;

    private String buf;
    private static final int THRESHOLD = 1024; 

    public OtusReporter() {
        buf = new String();
    }
   
    public void setProperties(Properties p) {
       try {
        address = InetAddress.getByName(
            p.getProperty("otus.address", "localhost"));
      } catch (Exception e) {
        System.out.println("Get default address wrong!");
      }
      port = Integer.parseInt(p.getProperty("otus.port", "10600"));
      try {
        host = p.getProperty("otus.hostname", InetAddress.getLocalHost().getHostName());
      } catch (Exception e) {
        System.out.println("Get hostname error!");
      }
    }

    public void start() {
        try {
          socket = new DatagramSocket();
        } catch (SocketException e) {
        }
    }

    public void end() {
        commit();
        socket.close();
    }

    public void commit()
    {
      try {
        DatagramPacket p = new DatagramPacket(buf.getBytes(), buf.length(),
          address, port);
        socket.send(p);
        buf = new String();
      } catch (IOException e) {
        System.out.println("Failed to send metric: "+e);
      } 
    }

    public void send(String name, double dvalue) {
        send(name, Double.toString(dvalue));
    }

    public void send(String name, int dvalue) {
        send(name, Integer.toString(dvalue));
    }

    public void send(String name, String value) {
        buf += name;
        buf += " ";
        buf += Long.toString(System.currentTimeMillis()/1000);
        buf += " ";
        buf += value;
        buf += "\n";
        if (buf.length() > THRESHOLD) {
          commit();
        }
    }

    public static void main(String args[]) throws Exception {
        OtusReporter rep = new OtusReporter();
        Properties prop = new Properties();
        rep.setProperties(prop);
        rep.start();
        int i = 0;
        int j = 0;
        while (j < 1000) {
          j = j + 1;
          i = i%10+1;
          rep.send("disk", i);
          rep.send("fly", i);
          try {
            Thread.sleep(10000);
          } catch (Exception e) {
          }
        }
        rep.end();
    }
}
