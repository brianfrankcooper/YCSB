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

import org.apache.hadoop.io.Text;

public class DataGenerator {

    String  format;
    long    start;
    long    current;
    long    end;
    boolean hashKey;
    int     hashedKeyRange;
    int     hashedKeyRangeStart;

    public DataGenerator( String rowprefix, long start, long end,
            boolean hashKey ) {
        this( rowprefix, start, end, hashKey, Integer.MAX_VALUE, 0 );
    }
    
    
    public DataGenerator( String rowprefix, long start, long end,
            boolean hashKey, int hashedKeyRange, int hashedKeyRangeStart ) {
	this.format  = getKeyFormat( rowprefix );
	this.start   = start;
	this.end     = end;
	this.current = start;
	this.hashKey = hashKey;
	this.hashedKeyRange = hashedKeyRange;
	this.hashedKeyRangeStart = hashedKeyRangeStart;
    }


    public static String getKeyFormat( String row_prefix ) {
	return row_prefix + "%010d";
    }

    public static Text generateRow(String format, int rowid, int startRow) {
	return new Text(String.format(format, rowid + startRow));
    }


    public String getNextKey() {
	if (this.current > this.end) {
	    return null;
	}

	long key = this.current;
        int  keyid = (int)key;
	this.current++;

	if (this.hashKey) { // hash the key
	    keyid = com.yahoo.ycsb.Utils.hash( keyid ) % this.hashedKeyRange
	        + this.hashedKeyRangeStart;
	}

	return String.format( this.format, keyid );
    }

    public Text getNextKeyText(Text t) {
        String key = this.getNextKey();

        if (key == null) {
            return null;
        }
        t.set( key );
        return t;
    }

    public Text getNextKeyText() {
        String key = this.getNextKey();

	return key == null ? null : new Text(key);
    }


    /**
     * Used as a stand alone data generator
     * @param args
     */
    public static void main(String[] args) {
        if (args.length != 7) {
            System.err.println( "Incorrect number of arguments: "
                        + args.length );
            System.err.println( "Usage: " + DataGenerator.class.getName() + 
                    " row_prefix start end datasize seed hashkey randomvalues" );
            System.exit( 1 );
        }
        

        /* arguments:
         *  String rowprefix, long start, long end, int seed, int datasize
         * boolean hashKey, boolean genRandomValues
         */
        String rowprefix = args[0];
        long start     = Long.parseLong( args[1]);
        long end       = Long.parseLong( args[2]);
        int  dataSize  = Integer.parseInt( args[3] );
        int  seed      = Integer.parseInt( args[4]);
        boolean hashkey = Boolean.parseBoolean( args[5] );
        boolean genRandomValues = Boolean.parseBoolean( args[6] );

        DataGenerator gen = new DataGenerator(rowprefix, start, end, hashkey);
        ValueGenerator vgen = new ValueGenerator(dataSize, genRandomValues, seed);
        Text key;
        long i = start;
        for (key = gen.getNextKeyText(); key != null; key = gen.getNextKeyText()) {
            System.out.print(  key );
            System.out.print(  ' ' );
            System.out.println( new String(vgen.getContentForCell( i, 0 )) );
            i++;
        }
    }

}
