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

import java.util.Random;

public class ValueGenerator {

    /* parameters */
    boolean genRandomValues;
    long    seed;
    int     datasize;
    
    /* internal state */
    byte[][] bytevals;
    byte[]   randomValue;
    Random   random;

    public ValueGenerator( int datasize, boolean genRandomValues, long seed ) {
        this.datasize = datasize;
        this.genRandomValues = genRandomValues;

        if (!genRandomValues) {
            this.bytevals = generateValues(datasize);
        } else {
            this.randomValue = new byte[datasize];
            this.random      = new Random();
        }
    }
    
    
    public byte[] getContentForCell( long row, int col ) {
        return this.genRandomValues 
            ? genRandomValue(this.random, this.randomValue, this.seed, row, col)
            : this.bytevals[col % bytevals.length];
    }


    public static byte[][] generateValues( int dataSize ) {
                
        byte[][] bytevals = new byte[10][];

        byte[] letters = { '1','2','3','4','5','6','7','8','9','0' };

        for (int i = 0; i < 10; i++) {
            bytevals[i] = new byte[dataSize];

            for (int j = 0; j < dataSize; j++)
                bytevals[i][j] = letters[i]; 
        }

        return bytevals;
    }


    public static byte[] genRandomValue( Random random, byte dest[], long seed,
                                         long row, int col )
    {
        random.setSeed((row ^ seed) ^ col);
        random.nextBytes(dest);
        toPrintableChars(dest);

        return dest;
    }

    /**
     * Transform the contents of a byte array into to printable characters.
     */
    public static void toPrintableChars(byte[] dest) {

        for (int i = 0; i < dest.length; i++) {
            dest[i] = (byte)(( (0xff & dest[i]) % 92) + ' ');
        }
    }

}
