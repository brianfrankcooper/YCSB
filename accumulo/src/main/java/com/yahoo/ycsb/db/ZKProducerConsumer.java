/**
 * Copyright (c) 2011 YCSB++ project, 2014 YCSB contributors.
 * All rights reserved.
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
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

/**
 * Implementing the PC (Producer/Consumer) Queue in ZooKeeper.
 */
public class ZKProducerConsumer implements Watcher {

  private static ZooKeeper zk = null;
  private static Integer mutex;

  private String root;

  /**
   * Constructor that takes the address of the ZK server.
   * 
   * @param address
   *          The address of the ZK server.
   */
  ZKProducerConsumer(String address) {
    if (zk == null) {
      try {
        System.out.println("Starting ZK:");
        zk = new ZooKeeper(address, 3000, this);
        mutex = new Integer(-1);
        System.out.println("Finished starting ZK: " + zk);
      } catch (IOException e) {
        System.out.println(e.toString());
        zk = null;
      }
    }
    // else mutex = new Integer(-1);
  }

  public synchronized void process(WatchedEvent event) {
    synchronized (mutex) {
      // System.out.println("Process: " + event.getType());
      mutex.notify();
    }
  }

  /**
   * Returns the root.
   *
   * @return The root.
   */
  protected String getRoot() {
    return root;
  }

  /**
   * Sets the root.
   * 
   * @param r
   *          The root value.
   */
  protected void setRoot(String r) {
    this.root = r;
  }

  /**
   * QueueElement a single queue element.  No longer used.
   * @deprecated No longer used.
   */
  @Deprecated
  public static class QueueElement {
    private String key;
    private long writeTime;

    QueueElement(String key, long writeTime) {
      this.key = key;
      this.writeTime = writeTime;
    }
  }

  /**
   * Producer-Consumer queue.
   */
  public static class Queue extends ZKProducerConsumer {

    /**
     * Constructor of producer-consumer queue.
     * 
     * @param address
     *          The Zookeeper server address.
     * @param name
     *          The name of the root element for the queue.
     */
    Queue(String address, String name) {
      super(address);
      this.setRoot(name);
      // Create ZK node name
      if (zk != null) {
        try {
          Stat s = zk.exists(getRoot(), false);
          if (s == null) {
            zk.create(getRoot(), new byte[0], Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
          }
        } catch (KeeperException e) {
          System.out.println(
              "Keeper exception when instantiating queue: " + e.toString());
        } catch (InterruptedException e) {
          System.out.println("Interrupted exception");
        }
      }
    }

    /**
     * Producer calls this method to insert the key in the queue.
     * 
     * @param key
     *          The key to produce (add to the queue).
     * @return True if the key was added.
     * @throws KeeperException
     *           On a failure talking to zookeeper.
     * @throws InterruptedException
     *           If the current thread is interrupted waiting for the zookeeper
     *           acknowledgement.
     */
    //
    boolean produce(String key) throws KeeperException, InterruptedException {
      byte[] value;
      value = key.getBytes();
      zk.create(getRoot() + "/key", value, Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT_SEQUENTIAL);

      return true;
    }

    /**
     * Consumer calls this method to "wait" for the key to the available.
     * 
     * @return The key to consumed (remove from the queue).
     * @throws KeeperException
     *           On a failure talking to zookeeper.
     * @throws InterruptedException
     *           If the current thread is interrupted waiting for the zookeeper
     *           acknowledgement.
     */
    String consume() throws KeeperException, InterruptedException {
      String retvalue = null;
      Stat stat = null;

      // Get the first element available
      while (true) {
        synchronized (mutex) {
          List<String> list = zk.getChildren(getRoot(), true);
          if (list.size() == 0) {
            System.out.println("Going to wait");
            mutex.wait();
          } else {
            String path = getRoot() + "/" + list.get(0);
            byte[] b = zk.getData(path, false, stat);
            retvalue = new String(b);
            zk.delete(path, -1);

            return retvalue;

          }
        }
      }
    }
  }
}
