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
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * One experiment scenario. One object of this type will
 * be instantiated and shared among all client threads. This class
 * should be constructed using a no-argument constructor, so we can
 * load it dynamically. Any argument-based initialization should be
 * done by init().
 * 
 * If you extend this class, you should support the "insertstart" property. This 
 * allows the load phase to proceed from multiple clients on different machines, in case
 * the client is the bottleneck. For example, if we want to load 1 million records from
 * 2 machines, the first machine should have insertstart=0 and the second insertstart=500000. Additionally,
 * the "insertcount" property, which is interpreted by Client, can be used to tell each instance of the
 * client how many inserts to do. In the example above, both clients should have insertcount=500000.
 */
public abstract class Workload
{
	public static final String INSERT_START_PROPERTY="insertstart";
	
	public static final String INSERT_START_PROPERTY_DEFAULT="0";
	
	private volatile AtomicBoolean stopRequested = new AtomicBoolean(false);
	
      /**
       * Initialize the scenario. Create any generators and other shared objects here.
       * Called once, in the main client thread, before any operations are started.
       */
      public void init(Properties p) throws WorkloadException
      {
      }

      /**
       * Initialize any state for a particular client thread. Since the scenario object
       * will be shared among all threads, this is the place to create any state that is specific
       * to one thread. To be clear, this means the returned object should be created anew on each
       * call to initThread(); do not return the same object multiple times. 
       * The returned object will be passed to invocations of doInsert() and doTransaction() 
       * for this thread. There should be no side effects from this call; all state should be encapsulated
       * in the returned object. If you have no state to retain for this thread, return null. (But if you have
       * no state to retain for this thread, probably you don't need to override initThread().)
       * 
       * @return false if the workload knows it is done for this thread. Client will terminate the thread. Return true otherwise. Return true for workloads that rely on operationcount. For workloads that read traces from a file, return true when there are more to do, false when you are done.
       */
      public Object initThread(Properties p, int mythreadid, int threadcount) throws WorkloadException
      {
	 return null;
      }
      
      /**
       * Cleanup the scenario. Called once, in the main client thread, after all operations have completed.
       */
      public void cleanup() throws WorkloadException
      {
      }
      
      /**
       * Do one insert operation. Because it will be called concurrently from multiple client threads, this 
       * function must be thread safe. However, avoid synchronized, or the threads will block waiting for each 
       * other, and it will be difficult to reach the target throughput. Ideally, this function would have no side
       * effects other than DB operations and mutations on threadstate. Mutations to threadstate do not need to be
       * synchronized, since each thread has its own threadstate instance.
       */
      public abstract boolean doInsert(DB db, Object threadstate);
      
      /**
       * Do one transaction operation. Because it will be called concurrently from multiple client threads, this 
       * function must be thread safe. However, avoid synchronized, or the threads will block waiting for each 
       * other, and it will be difficult to reach the target throughput. Ideally, this function would have no side
       * effects other than DB operations and mutations on threadstate. Mutations to threadstate do not need to be
       * synchronized, since each thread has its own threadstate instance.
       * 
       * @return false if the workload knows it is done for this thread. Client will terminate the thread. Return true otherwise. Return true for workloads that rely on operationcount. For workloads that read traces from a file, return true when there are more to do, false when you are done.
       */
      public abstract boolean doTransaction(DB db, Object threadstate);
      
      /**
       * Allows scheduling a request to stop the workload.
       */
      public void requestStop() {
        stopRequested.set(true);
      }
      
      /**
       * Check the status of the stop request flag.
       * @return true if stop was requested, false otherwise.
       */
      public boolean isStopRequested() {
        if (stopRequested.get() == true) return true;
        else return false;
      }
}
