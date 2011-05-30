/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.                                                                                                                             
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

import java.util.Vector;

/**
 * A thread that waits for the maximum specified time and then interrupts all the client
 * threads passed as the Vector at initialization of this thread.
 * 
 * The maximum execution time passed is assumed to be in seconds.
 * 
 * @author sudipto
 *
 */
public class TerminatorThread extends Thread {
  
  private Vector<Thread> threads;
  private long maxExecutionTime;
  
  public TerminatorThread(long maxExecutionTime, Vector<Thread> threads) {
    this.maxExecutionTime = maxExecutionTime;
    this.threads = threads;
    System.err.println("Maximum execution time specified as: " + maxExecutionTime + " secs");
  }
  
  public void run() {
    try {
      Thread.sleep(maxExecutionTime * 1000);
    } catch (InterruptedException e) {
      System.err.println("Could not wait until max specified time, thread interrupted.");
      return;
    }
    System.err.println("Maximum time elapsed. Interrupting the benchmark threads.");
    for (Thread t : threads) {
      if (!t.isInterrupted()) {
        t.interrupt();
      }
      try {
        t.join();
      } catch (InterruptedException e) {
      }
    }
  }
}
