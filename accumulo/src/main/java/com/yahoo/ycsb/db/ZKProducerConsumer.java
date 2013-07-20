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

// Implementing the PC Queue in ZooKeeper
//
public class ZKProducerConsumer implements Watcher {

    static ZooKeeper zk = null;
    static Integer mutex;

    String root;

    // Constructor that takes tha address of the ZK server
    //
    ZKProducerConsumer(String address) {
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


    static public class QueueElement {
        public String key;
        public long writeTime;

        QueueElement(String key, long writeTime) {
            this.key = key;
            this.writeTime = writeTime;
        }
    }

    // Producer-Consumer queue
    static public class Queue extends ZKProducerConsumer {

        // Constructor of producer-consumer queue
        Queue(String address, String name) {
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
                    System.out
                            .println("Keeper exception when instantiating queue: "
                                    + e.toString());
                } catch (InterruptedException e) {
                    System.out.println("Interrupted exception");
                }
            }
        }

        // Producer calls this method to insert the key in the queue
        //
        boolean produce(String key) throws KeeperException, InterruptedException{
            byte[] value;
            value = key.getBytes();
            zk.create(root + "/key", value, 
                      Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);

            return true;
        }

        // Consumer calls this method to "wait" for the key to the available
        //
        String consume() throws KeeperException, InterruptedException {
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
}
        
