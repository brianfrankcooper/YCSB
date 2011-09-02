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

public class GMetricReporter extends Reporter {

    public final static int SLOPE_ZERO         = 0;
    public final static int SLOPE_POSITIVE     = 1;
    public final static int SLOPE_NEGATIVE     = 2;
    public final static int SLOPE_BOTH         = 3;
    public final static int SLOPE_UNSPECIFIED  = 4;

    public final static String VALUE_STRING          = "string";
    public final static String VALUE_UNSIGNED_SHORT  = "uint16";
    public final static String VALUE_SHORT           = "int16";
    public final static String VALUE_UNSIGNED_INT    = "uint32";
    public final static String VALUE_INT             = "int32";
    public final static String VALUE_FLOAT           = "float";
    public final static String VALUE_DOUBLE          = "double";
    public final static String VALUE_TIMESTAMP       = "timestamp";

    private InetAddress address;
    private int port;
    private String host;
    private int tmax;
    private int dmax;
    private DatagramSocket socket;

    public GMetricReporter() {
    }
   
    public void setProperties(Properties p) {
       try {
        address = InetAddress.getByName(
            p.getProperty("gmetric.gmondaddress", "localhost"));
      } catch (Exception e) {
        System.out.println("Get default address wrong!");
      }
      port = Integer.parseInt(p.getProperty("gmetric.port", "8649"));
      try {
        host = p.getProperty("gmetric.hostname", InetAddress.getLocalHost().getHostName());
      } catch (Exception e) {
        System.out.println("Get hostname error!");
      }
      tmax = Integer.parseInt(p.getProperty("gmetric.tmax", "20"));
      dmax = Integer.parseInt(p.getProperty("gmetric.dmax", "10"));
    }

    public void start() {
        try {
          socket = new DatagramSocket();
        } catch (SocketException e) {
        }
    }

    public void end() {
        socket.close();
    }

    public void commit() {
    }

    public void send(String name, String value, String type,
                     String units, int slope, String group)
    {
      String realname=group+"."+name;
      try {
        byte[] buf = writemeta(host, realname, type, units, slope, tmax, dmax, group);
        DatagramPacket p = new DatagramPacket(buf, buf.length,
          address, port);
        socket.send(p);
        buf = writevalue(host, realname, value);
        p = new DatagramPacket(buf, buf.length, address, port);
        socket.send(p);
      } catch (Exception e) {
        System.out.println("Failed to send metric: "+e);
      }
    }

    public void send(String name, double dvalue) {
        send(name, Double.toString(dvalue), VALUE_DOUBLE,
            "", SLOPE_BOTH, "ycsb");
    }

    public void send(String name, int dvalue) {
        send(name, Integer.toString(dvalue), VALUE_INT,
            "", SLOPE_BOTH, "ycsb");
    }

    public void send(String name, String value) {
        send(name, value, VALUE_STRING,
            "", SLOPE_BOTH, "ycsb");
    }

    public byte[] writevalue(String host, String name, String val)
    {
	try {
	    ByteArrayOutputStream baos = new ByteArrayOutputStream();
	    DataOutputStream dos = new DataOutputStream(baos);
	    dos.writeInt(128+5);  // string
	    writeXDRString(dos, host);
	    writeXDRString(dos, name);
	    dos.writeInt(0);
	    writeXDRString(dos, "%s");
	    writeXDRString(dos, val);
	    return baos.toByteArray();
	} catch (IOException e) {
	    return null;
	}
    }

    public byte[] writemeta(String host, String name, String type,
        String units, int slope, int tmax, int dmax, String group)
    {
	try {
	    ByteArrayOutputStream baos = new ByteArrayOutputStream();
	    DataOutputStream dos = new DataOutputStream(baos);
	    dos.writeInt(128); 
            writeXDRString(dos, host);
	    writeXDRString(dos, name);
	    dos.writeInt(0); 
	    writeXDRString(dos, type);
	    writeXDRString(dos, name);
	    writeXDRString(dos, units);
	    dos.writeInt(slope);
	    dos.writeInt(tmax);
	    dos.writeInt(dmax);
            if (group.equals("")) {
              dos.writeInt(0);
            } else {
              dos.writeInt(1);
              writeXDRString(dos, "GROUP");
              writeXDRString(dos, group);
            }
	    return baos.toByteArray();
	} catch (IOException e) {
	    return null;
	}
    }

    private void writeXDRString(DataOutputStream dos, String s)
	throws IOException
    {
	dos.writeInt(s.length());
	dos.writeBytes(s);
	int offset = s.length() % 4;
	if (offset != 0) {
	    for (int i = offset; i < 4; ++i) {
		dos.writeByte(0);
	    }
	}
    }

    private static final byte[] HEXCHARS = {
	(byte)'0', (byte)'1', (byte)'2', (byte)'3',
	(byte)'4', (byte)'5', (byte)'6', (byte)'7',
	(byte)'8', (byte)'9', (byte)'a', (byte)'b',
	(byte)'c', (byte)'d', (byte)'e', (byte)'f'
    };

    private String bytes2hex(byte[] raw)
    {
	try {
	    int pos = 0;
	    byte[] hex = new byte[2 * raw.length];
	    for (int i = 0; i < raw.length; ++i) {
		int v = raw[i] & 0xFF;
		hex[pos++] = HEXCHARS[v >>> 4];
		hex[pos++] = HEXCHARS[v & 0xF];
	    }

	    return new String(hex, "ASCII");
	} catch (UnsupportedEncodingException e) {
	    return "";
	}
    }

    public static void main(String args[]) throws Exception {
        GMetricReporter gmetric = new GMetricReporter();
        int i = 0;
        while (true) {
          i = i%10+1;
          gmetric.send("disk", i);
          gmetric.send("fly", i);
          try {
            Thread.sleep(10000);
          } catch (Exception e) {
          }
        }
    }
}
