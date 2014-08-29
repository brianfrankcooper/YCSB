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

package com.yahoo.ycsb.generator;

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A key number generator that separately tracks keys for writing and reading.  Key numbers for writing use a simple
 * incrementing AtomicInteger.  For reads, the highest contiguous key number for which an insert has been completed
 * is used.  For example, if insertions have completed for [0, 1, 2, 5], then 2 should be used as the highest available
 * key number for reads, because writes for 3 and 4 have not completed yet.
 */
public class KeynumGenerator
{
    /**
     * Tracks the highest key number for which an insert has been started.
     */
    private final AtomicInteger submittedCounter;

    /**
     * Tracks the highest contiguous key number that should be available for reads.
     */
    private int highestContiguousCompleted;

    /**
     * A min-heap priority queue for tracking inserts that are in progress.
     */
    private final PriorityBlockingQueue<Integer> inProgress;

    /**
     * @param startAt the first value that should be used for inserts
     */
    public KeynumGenerator(int startAt)
    {
        submittedCounter = new AtomicInteger(startAt);
        inProgress = new PriorityBlockingQueue<Integer>();

        // ideally this would be null until the first insert had completed, but that causes problems with workloads
        this.highestContiguousCompleted = startAt;
    }

    /**
     * Start an insert.  Once called, completeInsert() must always be called with the return value of this method.
     * @return the next key number that should be used for inserts.
     */
    public int startInsert()
    {
        int nextKeyNumber = submittedCounter.getAndIncrement();
        inProgress.add(nextKeyNumber);
        return nextKeyNumber;
    }

    /**
     * Signal that an insert has completed.  This may result in the highest key number available for reads being
     * incremented.
     * @param keynum the key number that was inserted
     */
    public void completeInsert(int keynum)
    {
        // remove from the in-progress queue before holding the lock
        boolean didRemove = inProgress.remove(keynum);
        assert didRemove : "Completed keynum was not in in-progress priority queue";

        synchronized (this)
        {
            // while holding the lock, see if we may be the new highest contiguous key number
            if (keynum > highestContiguousCompleted)
            {
                // get the lowest key number from the in-progress heap
                Integer lowest = inProgress.peek();

                // if there is nothing in progress, or everything in progress is a higher key number, set the new
                // highest contiguous key number
                if (lowest == null || lowest > keynum)
                    highestContiguousCompleted = keynum;
            }
        }
    }

    /**
     * @return the highest key number available for reads.
     */
    public int getKeynumForRead()
    {
        return highestContiguousCompleted;
    }
}
