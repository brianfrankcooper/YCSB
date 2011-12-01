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

import java.util.Iterator;
import java.util.ArrayList;
/**
 * YCSB-specific buffer class.  ByteIterators are designed to support
 * efficient field generation, and to allow backend drivers that can stream
 * fields (instead of materializing them in RAM) to do so.
 * <p>
 * YCSB originially used String objects to represent field values.  This led to
 * two performance issues.
 * </p><p>
 * First, it leads to unnecessary conversions between UTF-16 and UTF-8, both
 * during field generation, and when passing data to byte-based backend
 * drivers.
 * </p><p>
 * Second, Java strings are represented internally using UTF-16, and are
 * built by appending to a growable array type (StringBuilder or
 * StringBuffer), then calling a toString() method.  This leads to a 4x memory
 * overhead as field values are being built, which prevented YCSB from
 * driving large object stores.
 * </p>
 * The StringByteIterator class contains a number of convenience methods for
 * backend drivers that convert between Map&lt;String,String&gt; and
 * Map&lt;String,ByteBuffer&gt;.
 *
 * @author sears
 */
public abstract class ByteIterator implements Iterator<Byte> {

	@Override
	public abstract boolean hasNext();

	@Override
	public Byte next() {
		throw new UnsupportedOperationException();
		//return nextByte();
	}

	public abstract byte nextByte();
        /** @return byte offset immediately after the last valid byte */
	public int nextBuf(byte[] buf, int buf_off) {
		int sz = buf_off;
		while(sz < buf.length && hasNext()) {
			buf[sz] = nextByte();
			sz++;
		}
		return sz;
	}

	public abstract long bytesLeft();
	
	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	/** Consumes remaining contents of this object, and returns them as a string. */
	public String toString() {
		StringBuilder sb = new StringBuilder();
		while(this.hasNext()) { sb.append((char)nextByte()); }
		return sb.toString();
	}
	/** Consumes remaining contents of this object, and returns them as a byte array. */
	public byte[] toArray() {
	    long left = bytesLeft();
	    if(left != (int)left) { throw new ArrayIndexOutOfBoundsException("Too much data to fit in one array!"); }
	    byte[] ret = new byte[(int)left];
	    int off = 0;
	    while(off < ret.length) {
		off = nextBuf(ret, off);
	    }
	    return ret;
	}

}
