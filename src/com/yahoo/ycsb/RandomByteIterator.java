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

import java.util.Random;

public class RandomByteIterator extends ByteIterator {
	static Random random=new Random();
	long len;
	long off;
	int buf_off;
	byte[] buf;
	
	@Override
	public boolean hasNext() {
		return (off + buf_off) < len;
	}

	private void fillBytesImpl(byte[] buf, int base) {
		int bytes = random.nextInt();
		try {
			buf[base+0] = (byte)(((bytes      ) & 31) + ' ');
			buf[base+1] = (byte)(((bytes >> 5 ) & 31) + ' ');
			buf[base+2] = (byte)(((bytes >> 10) & 31) + ' ');
			buf[base+3] = (byte)(((bytes >> 15) & 31) + ' ');
			buf[base+4] = (byte)(((bytes >> 20) & 31) + ' ');
			buf[base+5] = (byte)(((bytes >> 25) & 31) + ' ');
		} catch (ArrayIndexOutOfBoundsException e) { /* ignore it */ }
	}
	
	private void fillBytes() {
		if(buf_off ==  buf.length) {
			fillBytesImpl(buf, 0);
			buf_off = 0;
			off += buf.length;
		}
	}
	
	public RandomByteIterator(long len) {
		this.len = len;
		this.buf = new byte[6];
		this.buf_off = buf.length;
		fillBytes();
		this.off = 0;
	}

	public byte nextByte() {
		fillBytes();
		buf_off++;
		return buf[buf_off-1];
	}
	@Override
	public int nextBuf(byte[] b, int buf_off) {
		int ret;
		if(len - off < b.length - buf_off) {
			ret = (int)(len - off);
		} else {
			ret = b.length - buf_off;
		}
		int i;
		for(i = 0; i < ret; i+=6) {
			fillBytesImpl(b, i+buf_off);
		}
		off+=ret;
		return ret + buf_off;
	}
	
	
	@Override
	public long bytesLeft() {
		return len - off;
	}
}
