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

package com.yahoo.ycsb;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

// Implementing the PC Queue in ZooKeeper
//
public class ZKCoordination implements Watcher {

    static ZooKeeper zk = null;
    static Integer mutex;

    String root;
    public String zkAddress;
    
    // Constructor that takes tha address of the ZK server
    //
    ZKCoordination(String address) {
    	zkAddress = address;
    	
        if(zk == null){
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
        //else mutex = new Integer(-1);
    }

    synchronized public void process(WatchedEvent event) {
        synchronized (mutex) {
            //System.out.println("Process: " + event.getType());
            mutex.notify();
        }
    }

    // Barrier synchronization class
    static public class Barrier extends ZKCoordination 
    {
        String BarrierRoot;
        int BarrierSize;

        String nodename;

        // constructor
        //
        public Barrier(String address, String name, int size)
        {
            super(address);
            //this.root = name;
            //this.size = size;
            BarrierRoot = name;
            BarrierSize = size;
            
            // Create barrier node
            if (zk != null) {
                try {
                    Stat s = zk.exists(BarrierRoot, false);
                    if (s == null) {
                        zk.create(BarrierRoot, new byte[0],
                                  Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                } catch (KeeperException e) {
                    System.out.println("KeeperException @ instantiating queue:"
                                       + e.toString());
                } catch (InterruptedException e) {
                    System.out.println("InterruptedException:" + e.toString());
                }
            }

            // My node name
            try {
                nodename = new String(InetAddress.getLocalHost().getCanonicalHostName().toString());
            } catch (UnknownHostException e) {
            }
        }
        
        // join the barrier
        //
        public boolean enter() throws KeeperException, InterruptedException //throws KeeperException, InterruptedException
, IOException
        {
        	boolean flag;
        	do {
        		flag  = false;
        		try {
        			zk.create(BarrierRoot + "/" + nodename, new byte[0], 
        					Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        		} catch (KeeperException e) {
        			// TODO Auto-generated catch block

        			e.printStackTrace();
        			if (e.code() == Code.SESSIONEXPIRED){
        				System.out.println("I'm going to retry....");
        				flag = true;
        				zk = new ZooKeeper(zkAddress, 3000, this);
        			}
        		} catch (InterruptedException e) {
        			// TODO Auto-generated catch block
        			e.printStackTrace();
        		}
        	} while (flag);


        	System.out.println("***Enter="+BarrierRoot+"/"+nodename);
            while (true) {
            	synchronized (mutex) {
                    List<String> list = zk.getChildren(BarrierRoot, true);
                    System.out.println("***Size="+list.size()+";Needed="+BarrierSize);
                    if (list.size() < BarrierSize)
                        mutex.wait();
                    else
                        return true;
                }
            }
        }

        // wait until all processes reach the barrier
        //
        public boolean leave() throws KeeperException, InterruptedException, IOException
        {
        	boolean flag = false;
        	do{
        		flag  = false;
        		try {
        			zk.delete(BarrierRoot + "/" + nodename, 0);
        		} catch (InterruptedException e) {
        			// TODO Auto-generated catch block
        			e.printStackTrace();
        		} catch (KeeperException e) {
        			// TODO Auto-generated catch block
        			e.printStackTrace();
        			if (e.code() == Code.SESSIONEXPIRED){
        				System.out.println("I'm going to retry....");
        				flag = true;
        				zk = new ZooKeeper(zkAddress, 3000, this);
        			}
        		}
        	} while (flag);
            System.out.println("***Leave="+BarrierRoot+"/"+nodename);
            while (true) {
                synchronized (mutex) {
                    List<String> list = zk.getChildren(BarrierRoot, true);
                    System.out.println("***Size="+list.size()+";Needed=0");
                    if (list.size() > 0) 
                        mutex.wait();
                    else 
                        return true;
                }
            }
        }

    }


    // Producer-Consumer queue
    static public class Queue extends ZKCoordination {

        // Constructor of producer-consumer queue
        public Queue(String address, String name) {
            super(address);
            this.root = name;
            // Create ZK node name
            if (zk != null) {
                try {
                    Stat s = zk.exists(root, false);
                    if (s == null) {
                        zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE,
                                  CreateMode.PERSISTENT);
                    }
                } catch (KeeperException e) {
                    System.out.println("KeeperException @ instantiating queue:"
                                       + e.toString());
                } catch (InterruptedException e) {
                    System.out.println("InterruptedException:" + e.toString());
                }
            }
        }

        // Producer calls this method to insert the key in the queue
        //
        public boolean produce(String key) throws KeeperException, InterruptedException{
            byte[] value;
            value = key.getBytes();
            zk.create(root + "/key", value, 
                      Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);

            return true;
        }

        // Consumer calls this method to "wait" for the key to the available
        //
        public String consume() throws KeeperException, InterruptedException {
            String retvalue = null;
            Stat stat = null;

            // Get the first element available
            while (true) {
                synchronized (mutex) {
                    List<String> list = zk.getChildren(root, true);
                    if (list.size() == 0) {
                        System.out.println("Going to wait");
                        mutex.wait();
                    } else {
                        String path = root+"/"+list.get(0);
                        byte[] b = zk.getData(path, false, stat);
                        retvalue = new String(b);
                        zk.delete(path, -1);

                        return retvalue;

                    }
                }
            }
        }
    }
    
    static public class QueueElement 
    {
        public String key;
        public long writeTime;

        QueueElement(String key, long writeTime) 
        {
            this.key = key;
            this.writeTime = writeTime;
        }
    }
}
        
