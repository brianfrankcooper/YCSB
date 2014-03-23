/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
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

package com.yahoo.ycsb;

import java.util.Properties;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility functions.
 */
public class Utils
{
  private final static Pattern NUMBER_PATTERN = Pattern.compile("^([0-9\\.]*)([kKmMbBtT])*$");
  private static final Random rand = new Random();
  private static final ThreadLocal<Random> rng = new ThreadLocal<Random>();

  public static Random random() {
    Random ret = rng.get();
    if(ret == null) {
      ret = new Random(rand.nextLong());
      rng.set(ret);
    }
    return ret;
  }
  /**
   * Generate a random ASCII string of a given length.
   */
  public static String ASCIIString(int length)
  {
    int interval='~'-' '+1;

    byte []buf = new byte[length];
    random().nextBytes(buf);
    for (int i = 0; i < length; i++) {
      if (buf[i] < 0) {
        buf[i] = (byte)((-buf[i] % interval) + ' ');
      } else {
        buf[i] = (byte)((buf[i] % interval) + ' ');
      }
    }
    return new String(buf);
  }

  /**
   * Hash an integer value.
   */
  public static long hash(long val)
  {
    return FNVhash64(val);
  }

  public static final int FNV_offset_basis_32=0x811c9dc5;
  public static final int FNV_prime_32=16777619;

  /**
   * 32 bit FNV hash. Produces more "random" hashes than (say) String.hashCode().
   *
   * @param val The value to hash.
   * @return The hash value
   */
  public static int FNVhash32(int val)
  {
    //from http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
    int hashval = FNV_offset_basis_32;

    for (int i=0; i<4; i++)
    {
      int octet=val&0x00ff;
      val=val>>8;

    hashval = hashval ^ octet;
    hashval = hashval * FNV_prime_32;
    //hashval = hashval ^ octet;
    }
    return Math.abs(hashval);
  }

  public static final long FNV_offset_basis_64=0xCBF29CE484222325L;
  public static final long FNV_prime_64=1099511628211L;

  /**
   * 64 bit FNV hash. Produces more "random" hashes than (say) String.hashCode().
   *
   * @param val The value to hash.
   * @return The hash value
   */
  public static long FNVhash64(long val)
  {
    //from http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
    long hashval = FNV_offset_basis_64;

    for (int i=0; i<8; i++)
    {
      long octet=val&0x00ff;
      val=val>>8;

    hashval = hashval ^ octet;
    hashval = hashval * FNV_prime_64;
    //hashval = hashval ^ octet;
    }
    return Math.abs(hashval);
  }

  public static boolean getPropertyBool(Properties _p, String key, Boolean defaultValue)
  {
    return _p != null
        ? Boolean.parseBoolean(_p.getProperty(key, defaultValue.toString()))
            : defaultValue;
  }

  public static int getPropertyInt(Properties _p, String key, int defaultValue) {
    return (int) getPropertyDouble(_p, key, defaultValue);
  }

  public static long getPropertyLong(Properties _p, String key,
      long defaultValue) {
    return (long) getPropertyDouble(_p, key, (long) defaultValue);
  }

  public static double getPropertyDouble(Properties _p, String key,
      double defaultValue) {
    double result = defaultValue;
    String val = null;
    if (_p != null && (val = _p.getProperty(key)) != null) {
      result = -1;
      try {
        Matcher matcher = NUMBER_PATTERN.matcher(val.trim());
        if (matcher.matches() && matcher.groupCount() == 2) {
          double number = Double.valueOf(matcher.group(1));
          long multiplier = 1;

          if (matcher.group(2) != null) {
            switch (matcher.group(2).toLowerCase().charAt(0)) {
            case 't':
              multiplier *= 1000;
            case 'b':
              multiplier *= 1000;
            case 'm':
              multiplier *= 1000;
            case 'k':
              multiplier *= 1000;
            }
          }
          result = (number * multiplier);
        }
      } catch (Throwable e) {
        result = -1;
      } finally {
        if (result == -1) {
          result = defaultValue;
          System.err.println("Invalid value '" + val
            + "' for property '" + key + "' assuming default.");
        }
      }
    }
    return result;
  }

  public static void main(String[] args) {
    Properties _p = new Properties();
    _p.setProperty("number", "12.234k");
    System.out.println(getPropertyDouble(_p, "number", 3));
    System.out.println(getPropertyInt(_p, "number", 3));
    System.out.println(getPropertyLong(_p, "number", 3));
  }
}
