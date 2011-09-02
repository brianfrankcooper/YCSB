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

package com.yahoo.ycsb.workloads;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;
import java.util.Vector;
import java.util.Random;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.DBFactory;
import com.yahoo.ycsb.UnknownDBException;
import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.UniformIntegerGenerator;
import com.yahoo.ycsb.measurements.Measurements;

import com.yahoo.ycsb.ZKCoordination;
import org.apache.zookeeper.KeeperException;

public class ClientCoordination extends CoreWorkload
{
    public static final String CLIENT_COORD_TYPE_PROPERTY="client-coord";
    public static final String CLIENT_COORD_TYPE_DEFAULT="none";
    public static final String CLIENT_COORD_TYPE_PRODUCER="producer";
    public static final String CLIENT_COORD_TYPE_CONSUMER="consumer";
    
    public static final String COORD_SERVER="coord-server";
    public static final String COORD_SERVER_ROOT="coord-server-zkRoot";
 
    private String coordination_flag = "";
    
    private ZKCoordination.Queue pc_q = null;
    private Random rand = null;
    
    private static Hashtable<String,Long> hmKeyReads = null;
    private static Hashtable<String,Integer> hmKeyNumReads = null;

    private static double SAMPLING_FRACTION = 0.01;
    private static int MIN_NUM_READ_ATTEMPTS = 1;
    private static boolean DBG_FLAG = false;

	  public void init(Properties p) throws WorkloadException
	  {
        super.init(p);
        coordination_flag = p.getProperty(CLIENT_COORD_TYPE_PROPERTY,
                                          CLIENT_COORD_TYPE_DEFAULT).trim();

        dbg_msg("*** init()");

        if (coordination_flag.equals(CLIENT_COORD_TYPE_PRODUCER) ||
            coordination_flag.equals(CLIENT_COORD_TYPE_CONSUMER)) {
            dbg_msg("*** YCSB CoordinationMode="+coordination_flag);
            String address = p.getProperty(COORD_SERVER);
            String root = p.getProperty(COORD_SERVER_ROOT);
            dbg_msg("*** COORD_INFO(server:"+address+";root="+root+")");
            pc_q = new ZKCoordination.Queue(address, root);
            rand = new Random();
        }
    }
	
    public boolean doInsert(DB db, Object threadstate) 
    {
        dbg_msg("*** doInsert()");

        int keynum=keysequence.nextInt();
        if (!orderedinserts) {
          keynum=Utils.hash(keynum);
        }
        String dbkey="user"+keynum;
        HashMap<String,String> values=new HashMap<String,String>();
        for (int i=0; i<fieldcount; i++) {
          String fieldkey="field"+i;
          String data=Utils.ASCIIString(fieldlength);
          values.put(fieldkey,data);
        }
        if (db.insert(table,dbkey,values) == 0) {
            //===============================================
            // svp: 
            // YCSB on a client produces the key to 
            // be stored in the shared queue in ZooKeeper.
            //
            //dbg_msg("Inserted Key =("+dbkey+") and PC_FLAG="+coordination_flag);
            if (coordination_flag.equals(CLIENT_COORD_TYPE_PRODUCER)) {
                if (rand.nextFloat() < SAMPLING_FRACTION)
                    produce_key(dbkey);
            }
            //===============================================

            return true;
        } else
            return false;
    }
    
    public void setDBParameters(DB db, Object threadstate) 
    {
        dbg_msg("*** setDBParameters()");

        if (coordination_flag.equals(CLIENT_COORD_TYPE_CONSUMER)) {
            hmKeyReads = new Hashtable<String,Long>();
            hmKeyNumReads = new Hashtable<String,Integer>();
            consume_key(db);
        }
    }
    
	
    public void doTransactionInsert(DB db)
	  {
        dbg_msg("*** doTransactionInsert()");

        //choose the next key
        int keynum=transactioninsertkeysequence.nextInt();
        if (!orderedinserts)
        {
          keynum=Utils.hash(keynum);
        }
        String dbkey="user"+keynum;
        
        HashMap<String,String> values=new HashMap<String,String>();
        for (int i=0; i<fieldcount; i++)
        {
          String fieldkey="field"+i;
          String data=Utils.ASCIIString(fieldlength);
          values.put(fieldkey,data);
        }
        //db.insert(table,dbkey,values);
        if (db.insert(table,dbkey,values) == 0) {
            //===============================================
            // svp: 
            // YCSB on a client produces the key to 
            // be stored in the shared queue in ZooKeeper.
            //
            dbg_msg("Inserted Key =("+dbkey+") and PC_FLAG="+coordination_flag);
            if (coordination_flag.equals(CLIENT_COORD_TYPE_PRODUCER)) {
                if (rand.nextFloat() < SAMPLING_FRACTION)
                    produce_key(dbkey);
            }
            //===============================================
        } 
    }

    private void produce_key(String key)
    {
        dbg_msg("*** produce_key()");

        if (coordination_flag.equals(CLIENT_COORD_TYPE_PRODUCER)) {
            dbg_msg("Producer ... ");
            try {
                dbg_msg("Produce:" + key);
                pc_q.produce(key);
            } catch (KeeperException e) {
            } catch (InterruptedException e) {
            }
        } 
    }

    private void consume_key(DB db)
    {
        dbg_msg("*** consumer_key()");

        if (coordination_flag.equals(CLIENT_COORD_TYPE_CONSUMER)) {
            dbg_msg("Consumer ... ");
            //XXX: do something better to keep the loop going (while??)
            while (true) {
                try {
                    String strKey = pc_q.consume();
                    dbg_msg("Consume:" + strKey);
                    if ((hmKeyReads.containsKey(strKey) == false) && 
                        (hmKeyNumReads.containsKey(strKey) == false)) { 
                        hmKeyReads.put(strKey, new Long(System.currentTimeMillis()));
                        hmKeyNumReads.put(strKey, new Integer(1));
                    }
                    
                    //YCSB Consumer will read the key that was fetched from the 
                    //queue in ZooKeeper.
                    //XXX:current way is kind of ugly but works, i think)
                    String table = "usertable";
                    HashSet<String> fields = new HashSet<String>();
                    for (int j=0; j<fieldcount; j++)
                        fields.add("field"+j);
                    HashMap<String,String> result = new HashMap<String,String>();

                    db.read(table, strKey, fields, result); 
                    dbg_msg("Key="+strKey+";ResultSize="+result.size());
                    
                    //If the results are empty, the key is enqueued in Zookeeper
                    //and tried again, until the results are found.
                    if (result.size() == 0) {
                        pc_q.produce(strKey);
                        int count = ((Integer)hmKeyNumReads.get(strKey)).intValue(); 
                        hmKeyNumReads.put(strKey, new Integer(count+1));
                    }
                    else {
                        if (((Integer)hmKeyNumReads.get(strKey)).intValue() > MIN_NUM_READ_ATTEMPTS) { 
                            long currTime = System.currentTimeMillis();
                            long writeTime = ((Long)hmKeyReads.get(strKey)).longValue();
                            System.out.println("Key="+strKey+
                                               //";StartSearch="+writeTime+
                                               //";EndSearch="+currTime+
                                               ";TimeLag="+(currTime-writeTime));
                        }
                    }
                } catch (KeeperException e) {
                } catch (InterruptedException e) {
                }
            }
        }
    }

    private void dbg_msg(String msg)
    {
        if (DBG_FLAG)
            System.out.println(msg);
    }


}
