/*
 * Copyright (c) 2012 YCSB contributors. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkLockOperation;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.boot.ZookeeperBootComponent;
import de.hhu.bsinfo.dxram.boot.ZookeeperBootComponentConfig;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkComponentConfig;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.engine.*;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponentConfig;
import de.hhu.bsinfo.dxram.util.NodeCapabilities;
import de.hhu.bsinfo.dxutils.NodeID;

/**
 * YCSB binding for DXRAM.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 09.11.2018
 * @author Kevin Beineke, kevin.beineke@hhu.de, 22.04.2017
 */
public class DXRAMClient extends DB {
  private static final AtomicInteger THREAD_COUNT = new AtomicInteger(0);
  private static final CountDownLatch LATCH_INIT = new CountDownLatch(1);

  private static DXRAMProperties properties;
  private static List<Short> storageNodes;
  private static AtomicInteger threadsActive;
  private static YCSBObjectPool objectPool;
  private static ChunkIDConverter chunkIDConverter;

  private static DXRAM client;
  private static BootService bootService;
  private static ChunkService chunkService;

  private int threadCounterValue;

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    if (threadEnter()) {
      System.out.println("Initializing client instance");
      properties = new DXRAMProperties(getProperties());
      initClientInstance();
      searchStorageNodes();

      objectPool = new YCSBObjectPool(properties.getThreadCount(), properties.getFieldsPerKey(),
          properties.getSizeOfField(), properties.usePooling());
      threadsActive = new AtomicInteger(properties.getThreadCount());
      chunkIDConverter = new ChunkIDConverter(storageNodes, properties.getRecordCount(),
          properties.getDistributionStrategy());
      LATCH_INIT.countDown();
    } else {
      try {
        LATCH_INIT.await();
      } catch (InterruptedException e) {
        throw new DBException("Awaiting initialization failed", e);
      }
    }
  }

  @Override
  public void cleanup() {
    if (threadLeave()) {
      System.out.println("Shutting down client instance...");
      client.shutdown();
    }
  }

  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
    YCSBObject object = objectPool.get();

    long chunkId = chunkIDConverter.toChunkId(key);

    chunkService.create().create((short) (chunkId >> 48), object);

    object.setID(chunkId);

    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      object.setFieldValue(entry.getKey(), entry.getValue());
    }

    int retries = 0;

    while (!chunkService.put().put(object) && retries < properties.getMaxRetries()) {
      System.out.printf("Putting chunk %s failed. Retrying (%d)...\n", ChunkID.toHexString(object.getID()), retries);
      retries++;
    }

    return retries < properties.getMaxRetries() ? Status.OK : Status.ERROR;
  }

  @Override
  public Status read(final String table, final String key, Set<String> fields, final Map<String, ByteIterator> result) {
    YCSBObject object = objectPool.get();

    object.setID(chunkIDConverter.toChunkId(key));

    int retries = 0;

    while (!chunkService.get().get(object, ChunkLockOperation.NONE) && retries < properties.getMaxRetries()) {
      System.out.printf("Getting chunk %s failed. Retrying (%d)...\n", ChunkID.toHexString(object.getID()), retries);
      retries++;
    }

    // read all fields
    if (fields == null) {
      for (int i = 0; i < properties.getFieldsPerKey(); i++) {
        result.put("field" + i, object.getFieldIterator(i));
      }
    } else {
      for (String field : fields) {
        result.put(field, object.getFieldIterator(field));
      }
    }

    return retries < properties.getMaxRetries() ? Status.OK : Status.ERROR;
  }

  @Override
  public Status update(final String table, final String key, final Map<String, ByteIterator> values) {
    YCSBObject object = objectPool.get();

    object.setID(chunkIDConverter.toChunkId(key));

    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      object.setFieldValue(entry.getKey(), entry.getValue());
    }

    int retries = 0;

    while (!chunkService.put().put(object, ChunkLockOperation.NONE) && retries < properties.getMaxRetries()) {
      System.out.printf("Putting chunk %s failed. Retrying (%d)...\n", ChunkID.toHexString(object.getID()), retries);
      retries++;
    }

    return retries < properties.getMaxRetries() ? Status.OK : Status.ERROR;
  }

  @Override
  public Status delete(final String table, final String key) {
    YCSBObject object = objectPool.get();

    object.setID(chunkIDConverter.toChunkId(key));

    if (chunkService.remove().remove(object) != 1) {
      System.out.printf("Removing chunk %s failed.\n", ChunkID.toHexString(object.getID()));
      return Status.ERROR;
    }

    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  /**
   * Searches all storage nodes within the network.
   */
  private void searchStorageNodes() {
    // Get all available storage nodes
    storageNodes = bootService.getSupportingNodes(NodeCapabilities.STORAGE);

    // Sort the list, so that all client instances use the same order
    Collections.sort(storageNodes);

    System.out.println("Found storage nodes " + storageNodes.stream()
        .map(NodeID::toHexString)
        .collect(Collectors.toList()));
  }

  /**
   * Initializes this client's DXRAM instance.
   */
  private void initClientInstance() {
    System.out.println("Creating DXRAM client instance");

    client = new DXRAM();

    DXRAMConfig dxramConfig = client.createDefaultConfigInstance();
    DXRAMEngineConfig engineConfig = dxramConfig.getEngineConfig();
    ZookeeperBootComponentConfig bootComponentConfig = dxramConfig.getComponentConfig(ZookeeperBootComponent.class);
    ChunkComponentConfig chunkComponentConfig = dxramConfig.getComponentConfig(ChunkComponent.class);
    NetworkComponentConfig networkComponentConfig = dxramConfig.getComponentConfig(NetworkComponent.class);

    engineConfig.setRole("Peer");
    engineConfig.setAddress(properties.getBindAddress());
    bootComponentConfig.setConnection(properties.getJoinAddress());
    chunkComponentConfig.setChunkStorageEnabled(false);
    networkComponentConfig.getCoreConfig().setDevice(properties.getNetworkType());

    if (!client.initialize(dxramConfig, true)) {
      System.err.println("ERROR: Couldn't initialize DXRAM! Aborting.");
      System.exit(-1);
    }

    bootService = client.getService(BootService.class);
    chunkService = client.getService(ChunkService.class);
  }

  private boolean threadEnter() {
    threadCounterValue = THREAD_COUNT.incrementAndGet();
    return threadCounterValue == 1;
  }

  private boolean threadLeave() {
    return threadsActive.decrementAndGet() == 0;
  }
}
