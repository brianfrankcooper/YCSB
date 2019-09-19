/**
 * Copyright (c) 2017 YCSB contributors All rights reserved.
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
package site.ycsb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import site.ycsb.workloads.TimeSeriesWorkload;

/**
 * Basic DB for printing out time series workloads and/or tracking the distribution
 * of keys and fields.
 */
public class BasicTSDB extends BasicDB {

  /** Time series workload specific counters. */
  protected static Map<Long, Integer> timestamps;
  protected static Map<Integer, Integer> floats;
  protected static Map<Integer, Integer> integers;
  
  private String timestampKey;
  private String valueKey;
  private String tagPairDelimiter;
  private String queryTimeSpanDelimiter;
  private long lastTimestamp;
  
  @Override
  public void init() {
    super.init();
    
    synchronized (MUTEX) {
      if (timestamps == null) {
        timestamps = new HashMap<Long, Integer>();
        floats = new HashMap<Integer, Integer>();
        integers = new HashMap<Integer, Integer>();
      }
    }
    
    timestampKey = getProperties().getProperty(
        TimeSeriesWorkload.TIMESTAMP_KEY_PROPERTY, 
        TimeSeriesWorkload.TIMESTAMP_KEY_PROPERTY_DEFAULT);
    valueKey = getProperties().getProperty(
        TimeSeriesWorkload.VALUE_KEY_PROPERTY, 
        TimeSeriesWorkload.VALUE_KEY_PROPERTY_DEFAULT);
    tagPairDelimiter = getProperties().getProperty(
        TimeSeriesWorkload.PAIR_DELIMITER_PROPERTY, 
        TimeSeriesWorkload.PAIR_DELIMITER_PROPERTY_DEFAULT);
    queryTimeSpanDelimiter = getProperties().getProperty(
        TimeSeriesWorkload.QUERY_TIMESPAN_DELIMITER_PROPERTY,
        TimeSeriesWorkload.QUERY_TIMESPAN_DELIMITER_PROPERTY_DEFAULT);
  }
  
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    delay();
    
    if (verbose) {
      StringBuilder sb = getStringBuilder();
      sb.append("READ ").append(table).append(" ").append(key).append(" [ ");
      if (fields != null) {
        for (String f : fields) {
          sb.append(f).append(" ");
        }
      } else {
        sb.append("<all fields>");
      }

      sb.append("]");
      System.out.println(sb);
    }

    if (count) {
      Set<String> filtered = null;
      if (fields != null) {
        filtered = new HashSet<String>();
        for (final String field : fields) {
          if (field.startsWith(timestampKey)) {
            String[] parts = field.split(tagPairDelimiter);
            if (parts[1].contains(queryTimeSpanDelimiter)) {
              parts = parts[1].split(queryTimeSpanDelimiter);
              lastTimestamp = Long.parseLong(parts[0]);
            } else {
              lastTimestamp = Long.parseLong(parts[1]);
            }
            synchronized(timestamps) {
              Integer ctr = timestamps.get(lastTimestamp);
              if (ctr == null) {
                timestamps.put(lastTimestamp, 1);
              } else {
                timestamps.put(lastTimestamp, ctr + 1);
              }
            }
          } else {
            filtered.add(field);
          }
        }
      }
      incCounter(reads, hash(table, key, filtered));
    }
    return Status.OK;
  }
  
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    delay();

    boolean isFloat = false;
    
    if (verbose) {
      StringBuilder sb = getStringBuilder();
      sb.append("UPDATE ").append(table).append(" ").append(key).append(" [ ");
      if (values != null) {
        final TreeMap<String, ByteIterator> tree = new TreeMap<String, ByteIterator>(values);
        for (Map.Entry<String, ByteIterator> entry : tree.entrySet()) {
          if (entry.getKey().equals(timestampKey)) {
            sb.append(entry.getKey()).append("=")
              .append(Utils.bytesToLong(entry.getValue().toArray())).append(" ");
          } else if (entry.getKey().equals(valueKey)) {
            final NumericByteIterator it = (NumericByteIterator) entry.getValue();
            isFloat = it.isFloatingPoint();
            sb.append(entry.getKey()).append("=")
               .append(isFloat ? it.getDouble() : it.getLong()).append(" ");
          } else {
            sb.append(entry.getKey()).append("=").append(entry.getValue()).append(" ");
          }
        }
      }
      sb.append("]");
      System.out.println(sb);
    }

    if (count) {
      if (!verbose) {
        isFloat = ((NumericByteIterator) values.get(valueKey)).isFloatingPoint();
      }
      int hash = hash(table, key, values);
      incCounter(updates, hash);
      synchronized(timestamps) {
        Integer ctr = timestamps.get(lastTimestamp);
        if (ctr == null) {
          timestamps.put(lastTimestamp, 1);
        } else {
          timestamps.put(lastTimestamp, ctr + 1);
        }
      }
      if (isFloat) {
        incCounter(floats, hash);
      } else {
        incCounter(integers, hash);
      }
    }
    
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    delay();
    
    boolean isFloat = false;
    
    if (verbose) {
      StringBuilder sb = getStringBuilder();
      sb.append("INSERT ").append(table).append(" ").append(key).append(" [ ");
      if (values != null) {
        final TreeMap<String, ByteIterator> tree = new TreeMap<String, ByteIterator>(values);
        for (Map.Entry<String, ByteIterator> entry : tree.entrySet()) {
          if (entry.getKey().equals(timestampKey)) {
            sb.append(entry.getKey()).append("=")
              .append(Utils.bytesToLong(entry.getValue().toArray())).append(" ");
          } else if (entry.getKey().equals(valueKey)) {
            final NumericByteIterator it = (NumericByteIterator) entry.getValue();
            isFloat = it.isFloatingPoint();
            sb.append(entry.getKey()).append("=")
              .append(isFloat ? it.getDouble() : it.getLong()).append(" ");
          } else {
            sb.append(entry.getKey()).append("=").append(entry.getValue()).append(" ");
          }
        }
      }
      sb.append("]");
      System.out.println(sb);
    }

    if (count) {
      if (!verbose) {
        isFloat = ((NumericByteIterator) values.get(valueKey)).isFloatingPoint();
      }
      int hash = hash(table, key, values);
      incCounter(inserts, hash);
      synchronized(timestamps) {
        Integer ctr = timestamps.get(lastTimestamp);
        if (ctr == null) {
          timestamps.put(lastTimestamp, 1);
        } else {
          timestamps.put(lastTimestamp, ctr + 1);
        }
      }
      if (isFloat) {
        incCounter(floats, hash);
      } else {
        incCounter(integers, hash);
      }
    }

    return Status.OK;
  }

  @Override
  public void cleanup() {
    super.cleanup();
    if (count && counter < 1) {
      System.out.println("[TIMESTAMPS], Unique, " + timestamps.size());
      System.out.println("[FLOATS], Unique series, " + floats.size());
      System.out.println("[INTEGERS], Unique series, " + integers.size());
      
      long minTs = Long.MAX_VALUE;
      long maxTs = Long.MIN_VALUE;
      for (final long ts : timestamps.keySet()) {
        if (ts > maxTs) {
          maxTs = ts;
        }
        if (ts < minTs) {
          minTs = ts;
        }
      }
      System.out.println("[TIMESTAMPS], Min, " + minTs);
      System.out.println("[TIMESTAMPS], Max, " + maxTs);
    }
  }
  
  @Override
  protected int hash(final String table, final String key, final Map<String, ByteIterator> values) {
    final TreeMap<String, ByteIterator> sorted = new TreeMap<String, ByteIterator>();
    for (final Entry<String, ByteIterator> entry : values.entrySet()) {
      if (entry.getKey().equals(valueKey)) {
        continue;
      } else if (entry.getKey().equals(timestampKey)) {
        lastTimestamp = ((NumericByteIterator) entry.getValue()).getLong();
        entry.getValue().reset();
        continue;
      }
      sorted.put(entry.getKey(), entry.getValue());
    }
    // yeah it's ugly but gives us a unique hash without having to add hashers
    // to all of the ByteIterators.
    StringBuilder buf = new StringBuilder().append(table).append(key);
    for (final Entry<String, ByteIterator> entry : sorted.entrySet()) {
      entry.getValue().reset();
      buf.append(entry.getKey())
         .append(entry.getValue().toString());
    }
    return buf.toString().hashCode();
  }
  
}