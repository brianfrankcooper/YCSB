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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.LineReader;

/**
 * Port of org.apache.hadoop.mapred.lib.NLineInputFormat to the new MR
 * interface.
 */
public class NLineInputFormat extends TextInputFormat implements Configurable {

    private int N = 1;
    protected Configuration conf;

    protected int addFileLines( Path fileName, List<InputSplit> splits,
                                Configuration conf )
            throws IOException {
        FileSystem  fs = fileName.getFileSystem( conf );
        LineReader lr  = null;

        try {
            FSDataInputStream in = fs.open(fileName);
            lr = new LineReader(in, conf);
            Text line     = new Text();
            int  numLines = 0;
            long begin    = 0;
            long length   = 0;
            int  num      = -1;

            while ((num = lr.readLine(line)) > 0) {
                numLines++;
                length += num;
                if (numLines == N) {
                    splits.add(new FileSplit(fileName, begin, length, null));
                    begin    += length;
                    length   = 0;
                    numLines = 0;
                }
            }
            if (numLines != 0) { /* create split for last line(s) */
                splits.add(new FileSplit(fileName, begin, length, null));
            }
        } finally {
            if (lr != null) {
                lr.close();
            }
        }

        return -1;
    }

    /** 
     * Generate the list of files and make them into FileSplits.
     */ 
    public List<InputSplit> getSplits(JobContext job) throws IOException {

        ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
        for (FileStatus status : listStatus(job)) {
            Path fileName = status.getPath();
            if (status.isDir()) {
                throw new IOException("Not a file: " + fileName);
            }
            this.addFileLines( fileName, splits,  job.getConfiguration() );
        }
        return splits;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public void setConf( Configuration conf ) {
        this.conf = conf;
        this.N = conf.getInt("mapred.line.input.format.linespermap", 1);
    }    
}
