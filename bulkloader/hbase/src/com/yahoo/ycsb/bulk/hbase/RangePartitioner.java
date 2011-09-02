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

package com.yahoo.ycsb.bulk.hbase;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Scanner;
import java.util.TreeSet;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Hadoop partitioner that uses ranges, and optionally sub-bins based on
 * hashing.
 */
public class RangePartitioner extends Partitioner<Text, Writable> implements Configurable {
	private static final String PREFIX = RangePartitioner.class.getName();
	private static final String CUTFILE_KEY = PREFIX + ".cutFile";
	private static final String NUM_SUBBINS = PREFIX + ".subBins";

	private Configuration conf;

	public int getPartition(Text key, Writable value, int numPartitions) {
		try {
			return findPartition(key, getCutPoints(), getNumSubBins());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	int findPartition(Text key, Text[] array, int numSubBins) {
		// find the bin for the range, and guarantee it is positive
		int index = Arrays.binarySearch(array, key);
		index = index < 0 ? (index + 1) * -1 : index;

		// both conditions work with numSubBins == 1, but this check is to avoid
		// hashing, when we don't need to, for speed
		if (numSubBins < 2)
			return index;
		return (key.toString().hashCode() & Integer.MAX_VALUE) % numSubBins + index * numSubBins;
	}

	private int _numSubBins = 0;

	private synchronized int getNumSubBins() {
		if (_numSubBins < 1) {
			// get number of sub-bins and guarantee it is positive
			_numSubBins = Math.max(1, getConf().getInt(NUM_SUBBINS, 1));
		}
		return _numSubBins;
	}

	private Text cutPointArray[] = null;

	private synchronized Text[] getCutPoints() throws IOException {
		if (cutPointArray == null) {
			String cutFileName = conf.get(CUTFILE_KEY);
			Path[] cf = DistributedCache.getLocalCacheFiles(conf);

			if (cf != null) {
				for (Path path : cf) {
					if (path.toUri().getPath().endsWith(cutFileName.substring(cutFileName.lastIndexOf('/')))) {
						TreeSet<Text> cutPoints = new TreeSet<Text>();
						Scanner in = new Scanner(new BufferedReader(new FileReader(path.toString())));
						try {
							while (in.hasNextLine())
								cutPoints.add(new Text(Base64.decodeBase64(in.nextLine().getBytes())));
						} finally {
							in.close();
						}
						cutPointArray = cutPoints.toArray(new Text[cutPoints.size()]);
						break;
					}
				}
			}
			if (cutPointArray == null)
				throw new FileNotFoundException(cutFileName + " not found in distributed cache");
		}
		return cutPointArray;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	/**
	 * Sets the hdfs file name to use, containing a newline separated list of
	 * Base64 encoded split points that represent ranges for partitioning
	 */
	public static void setSplitFile(JobContext job, String file) {
		URI uri = new Path(file).toUri();
		DistributedCache.addCacheFile(uri, job.getConfiguration());
		job.getConfiguration().set(CUTFILE_KEY, uri.getPath());
	}

	/**
	 * Sets the number of random sub-bins per range
	 */
	public static void setNumSubBins(JobContext job, int num) {
		job.getConfiguration().setInt(NUM_SUBBINS, num);
	}
}
