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
package com.yahoo.ycsb.db;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ExecutionException;

import com.google.bigtable.repackaged.com.google.protobuf.ByteString;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.RowRange;
import com.google.bigtable.v2.RowSet;
import com.google.bigtable.v2.Mutation.DeleteFromRow;
import com.google.bigtable.v2.Mutation.SetCell;
import com.google.bigtable.v2.RowFilter.Chain.Builder;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.grpc.async.AsyncExecutor;
import com.google.cloud.bigtable.grpc.async.BulkMutation;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.util.ByteStringer;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

/**
 * Google Bigtable Proto client for YCSB framework.
 * 
 * Bigtable offers two APIs. These include a native Protobuf GRPC API as well as 
 * an HBase API wrapper for the GRPC API. This client implements the Protobuf 
 * API to test the underlying calls wrapped up in the HBase API. To use the 
 * HBase API, see the hbase10 client binding.
 */
public class GoogleBigtableClient extends com.yahoo.ycsb.DB {
  public static final Charset UTF8_CHARSET = Charset.forName("UTF8");
  
  /** Property names for the CLI. */
  private static final String ASYNC_MUTATOR_MAX_MEMORY = "mutatorMaxMemory";
  private static final String ASYNC_MAX_INFLIGHT_RPCS = "mutatorMaxInflightRPCs";
  private static final String CLIENT_SIDE_BUFFERING = "clientbuffering";
  
  /** Tracks running thread counts so we know when to close the session. */ 
  private static int threadCount = 0;
  
  /** This will load the hbase-site.xml config file and/or store CLI options. */
  private static final Configuration CONFIG = HBaseConfiguration.create();
  
  /** Print debug information to standard out. */
  private boolean debug = false;
  
  /** Global Bigtable native API objects. */ 
  private static BigtableOptions options;
  private static BigtableSession session;
  
  /** Thread loacal Bigtable native API objects. */
  private BigtableDataClient client;
  private AsyncExecutor asyncExecutor;
  
  /** The column family use for the workload. */
  private byte[] columnFamilyBytes;
  
  /** Cache for the last table name/ID to avoid byte conversions. */
  private String lastTable = "";
  private byte[] lastTableBytes;
  
  /**
   * If true, buffer mutations on the client. For measuring insert/update/delete 
   * latencies, client side buffering should be disabled.
   */
  private boolean clientSideBuffering = false;

  private BulkMutation bulkMutation;

  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    
    // Defaults the user can override if needed
    if (getProperties().containsKey(ASYNC_MUTATOR_MAX_MEMORY)) {
      CONFIG.set(BigtableOptionsFactory.BIGTABLE_BUFFERED_MUTATOR_MAX_MEMORY_KEY,
          getProperties().getProperty(ASYNC_MUTATOR_MAX_MEMORY));
    }
    if (getProperties().containsKey(ASYNC_MAX_INFLIGHT_RPCS)) {
      CONFIG.set(BigtableOptionsFactory.BIGTABLE_BULK_MAX_ROW_KEY_COUNT,
          getProperties().getProperty(ASYNC_MAX_INFLIGHT_RPCS));
    }
    // make it easy on ourselves by copying all CLI properties into the config object.
    final Iterator<Entry<Object, Object>> it = props.entrySet().iterator();
    while (it.hasNext()) {
      Entry<Object, Object> entry = it.next();
      CONFIG.set((String)entry.getKey(), (String)entry.getValue());
    }
    
    clientSideBuffering = getProperties().getProperty(CLIENT_SIDE_BUFFERING, "false")
        .equals("true") ? true : false;
    
    System.err.println("Running Google Bigtable with Proto API" +
         (clientSideBuffering ? " and client side buffering." : "."));
    
    synchronized (CONFIG) {
      ++threadCount;
      if (session == null) {
        try {
          options = BigtableOptionsFactory.fromConfiguration(CONFIG);
          session = new BigtableSession(options);
          // important to instantiate the first client here, otherwise the
          // other threads may receive an NPE from the options when they try
          // to read the cluster name.
          client = session.getDataClient();
        } catch (IOException e) {
          throw new DBException("Error loading options from config: ", e);
        }
      } else {
        client = session.getDataClient();
      }
      
      if (clientSideBuffering) {
        asyncExecutor = session.createAsyncExecutor();
      }
    }
    
    if ((getProperties().getProperty("debug") != null)
        && (getProperties().getProperty("debug").compareTo("true") == 0)) {
      debug = true;
    }
    
    final String columnFamily = getProperties().getProperty("columnfamily");
    if (columnFamily == null) {
      System.err.println("Error, must specify a columnfamily for Bigtable table");
      throw new DBException("No columnfamily specified");
    }
    columnFamilyBytes = Bytes.toBytes(columnFamily);
  }
  
  @Override
  public void cleanup() throws DBException {
    if (bulkMutation != null) {
      try {
        bulkMutation.flush();
      } catch(RuntimeException e){
        throw new DBException(e);
      }
    }
    if (asyncExecutor != null) {
      try {
        asyncExecutor.flush();
      } catch (IOException e) {
        throw new DBException(e);
      }
    }
    synchronized (CONFIG) {
      --threadCount;
      if (threadCount <= 0) {
        try {
          session.close();
        } catch (IOException e) {
          throw new DBException(e);
        }
      }
    }
  }
  
  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    if (debug) {
      System.out.println("Doing read from Bigtable columnfamily " 
          + new String(columnFamilyBytes));
      System.out.println("Doing read for key: " + key);
    }
    
    setTable(table);
    
    RowFilter filter = RowFilter.newBuilder()
        .setFamilyNameRegexFilterBytes(ByteStringer.wrap(columnFamilyBytes))
        .build();
    if (fields != null && fields.size() > 0) {
      Builder filterChain = RowFilter.Chain.newBuilder();
      filterChain.addFilters(filter);
      filterChain.addFilters(RowFilter.newBuilder()
          .setCellsPerColumnLimitFilter(1)
          .build());
      int count = 0;
      // usually "field#" so pre-alloc
      final StringBuilder regex = new StringBuilder(fields.size() * 6);
      for (final String field : fields) {
        if (count++ > 0) {
          regex.append("|");
        }
        regex.append(field);
      }
      filterChain.addFilters(RowFilter.newBuilder()
          .setColumnQualifierRegexFilter(
              ByteStringer.wrap(regex.toString().getBytes()))).build();
      filter = RowFilter.newBuilder().setChain(filterChain.build()).build();
    }
    
    final ReadRowsRequest.Builder rrr = ReadRowsRequest.newBuilder()
        .setTableNameBytes(ByteStringer.wrap(lastTableBytes))
        .setFilter(filter)
        .setRows(RowSet.newBuilder()
          .addRowKeys(ByteStringer.wrap(key.getBytes())));
    
    List<Row> rows;
    try {
      rows = client.readRowsAsync(rrr.build()).get();
      if (rows == null || rows.isEmpty()) {
        return Status.NOT_FOUND;
      }
      for (final Row row : rows) {
        for (final Family family : row.getFamiliesList()) {
          if (Arrays.equals(family.getNameBytes().toByteArray(), columnFamilyBytes)) {
            for (final Column column : family.getColumnsList()) {
              // we should only have a single cell per column
              result.put(column.getQualifier().toString(UTF8_CHARSET), 
                  new ByteArrayByteIterator(column.getCells(0).getValue().toByteArray()));
              if (debug) {
                System.out.println(
                    "Result for field: " + column.getQualifier().toString(UTF8_CHARSET)
                        + " is: " + column.getCells(0).getValue().toString(UTF8_CHARSET));
              }
            }
          }
        }
      }
      
      return Status.OK;
    } catch (InterruptedException e) {
      System.err.println("Interrupted during get: " + e);
      Thread.currentThread().interrupt();
      return Status.ERROR;
    } catch (ExecutionException e) {
      System.err.println("Exception during get: " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    setTable(table);
    
    RowFilter filter = RowFilter.newBuilder()
        .setFamilyNameRegexFilterBytes(ByteStringer.wrap(columnFamilyBytes))
        .build();
    if (fields != null && fields.size() > 0) {
      Builder filterChain = RowFilter.Chain.newBuilder();
      filterChain.addFilters(filter);
      filterChain.addFilters(RowFilter.newBuilder()
          .setCellsPerColumnLimitFilter(1)
          .build());
      int count = 0;
      // usually "field#" so pre-alloc
      final StringBuilder regex = new StringBuilder(fields.size() * 6);
      for (final String field : fields) {
        if (count++ > 0) {
          regex.append("|");
        }
        regex.append(field);
      }
      filterChain.addFilters(RowFilter.newBuilder()
          .setColumnQualifierRegexFilter(
              ByteStringer.wrap(regex.toString().getBytes()))).build();
      filter = RowFilter.newBuilder().setChain(filterChain.build()).build();
    }
    
    final RowRange range = RowRange.newBuilder()
        .setStartKeyClosed(ByteStringer.wrap(startkey.getBytes()))
        .build();

    final RowSet rowSet = RowSet.newBuilder()
        .addRowRanges(range)
        .build();

    final ReadRowsRequest.Builder rrr = ReadRowsRequest.newBuilder()
        .setTableNameBytes(ByteStringer.wrap(lastTableBytes))
        .setFilter(filter)
        .setRows(rowSet);
    
    List<Row> rows;
    try {
      rows = client.readRowsAsync(rrr.build()).get();
      if (rows == null || rows.isEmpty()) {
        return Status.NOT_FOUND;
      }
      int numResults = 0;
      
      for (final Row row : rows) {
        final HashMap<String, ByteIterator> rowResult =
            new HashMap<String, ByteIterator>(fields != null ? fields.size() : 10);
        
        for (final Family family : row.getFamiliesList()) {
          if (Arrays.equals(family.getNameBytes().toByteArray(), columnFamilyBytes)) {
            for (final Column column : family.getColumnsList()) {
              // we should only have a single cell per column
              rowResult.put(column.getQualifier().toString(UTF8_CHARSET), 
                  new ByteArrayByteIterator(column.getCells(0).getValue().toByteArray()));
              if (debug) {
                System.out.println(
                    "Result for field: " + column.getQualifier().toString(UTF8_CHARSET)
                        + " is: " + column.getCells(0).getValue().toString(UTF8_CHARSET));
              }
            }
          }
        }
        
        result.add(rowResult);
        
        numResults++;
        if (numResults >= recordcount) {// if hit recordcount, bail out
          break;
        }
      }
      return Status.OK;
    } catch (InterruptedException e) {
      System.err.println("Interrupted during scan: " + e);
      Thread.currentThread().interrupt();
      return Status.ERROR;
    } catch (ExecutionException e) {
      System.err.println("Exception during scan: " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    if (debug) {
      System.out.println("Setting up put for key: " + key);
    }
    
    setTable(table);
    
    final MutateRowRequest.Builder rowMutation = MutateRowRequest.newBuilder();
    rowMutation.setRowKey(ByteString.copyFromUtf8(key));
    rowMutation.setTableNameBytes(ByteStringer.wrap(lastTableBytes));
    
    for (final Entry<String, ByteIterator> entry : values.entrySet()) {
      final Mutation.Builder mutationBuilder = rowMutation.addMutationsBuilder();
      final SetCell.Builder setCellBuilder = mutationBuilder.getSetCellBuilder();
      
      setCellBuilder.setFamilyNameBytes(ByteStringer.wrap(columnFamilyBytes));
      setCellBuilder.setColumnQualifier(ByteStringer.wrap(entry.getKey().getBytes()));
      setCellBuilder.setValue(ByteStringer.wrap(entry.getValue().toArray()));

      // Bigtable uses a 1ms granularity
      setCellBuilder.setTimestampMicros(System.currentTimeMillis() * 1000);
    }
    
    try {
      if (clientSideBuffering) {
        bulkMutation.add(rowMutation.build());
      } else {
        client.mutateRow(rowMutation.build());
      }
      return Status.OK;
    } catch (RuntimeException e) {
      System.err.println("Failed to insert key: " + key + " " + e.getMessage());
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {
    return update(table, key, values);
  }

  @Override
  public Status delete(String table, String key) {
    if (debug) {
      System.out.println("Doing delete for key: " + key);
    }
    
    setTable(table);
    
    final MutateRowRequest.Builder rowMutation = MutateRowRequest.newBuilder()
        .setRowKey(ByteString.copyFromUtf8(key))
        .setTableNameBytes(ByteStringer.wrap(lastTableBytes));
    rowMutation.addMutationsBuilder().setDeleteFromRow(
        DeleteFromRow.getDefaultInstance());
    
    try {
      if (clientSideBuffering) {
        bulkMutation.add(rowMutation.build());
      } else {
        client.mutateRow(rowMutation.build());
      }
      return Status.OK;
    } catch (RuntimeException e) {
      System.err.println("Failed to delete key: " + key + " " + e.getMessage());
      return Status.ERROR;
    }
  }

  /**
   * Little helper to set the table byte array. If it's different than the last
   * table we reset the byte array. Otherwise we just use the existing array.
   * @param table The table we're operating against
   */
  private void setTable(final String table) {
    if (!lastTable.equals(table)) {
      lastTable = table;
      BigtableTableName tableName = options
          .getInstanceName()
          .toTableName(table);
      lastTableBytes = tableName
          .toString()
          .getBytes();
      synchronized(this) {
        if (bulkMutation != null) {
          bulkMutation.flush();
        }
        bulkMutation = session.createBulkMutation(tableName, asyncExecutor);
      }
    }
  }
  
}