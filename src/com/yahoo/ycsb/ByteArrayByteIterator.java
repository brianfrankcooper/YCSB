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

public class ByteArrayByteIterator extends ByteIterator {
	byte[] str;
	int off;
	final int len;
	public ByteArrayByteIterator(byte[] s) {
		this.str = s;
		this.off = 0;
		this.len = s.length;
	}

	public ByteArrayByteIterator(byte[] s, int off, int len) {
		this.str = s;
		this.off = off;
		this.len = off + len;
	}

	@Override
	public boolean hasNext() {
		return off < len;
	}

	@Override
	public byte nextByte() {
		byte ret = str[off];
		off++;
		return ret;
	}

	@Override
	public long bytesLeft() {
		return len - off;
	}

}
