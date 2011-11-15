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

import java.io.InputStream;

public class InputStreamByteIterator extends ByteIterator {
	long len;
	InputStream ins;
	long off;
	
	public InputStreamByteIterator(InputStream ins, long len) {
		this.len = len;
		this.ins = ins;
		off = 0;
	}
	
	@Override
	public boolean hasNext() {
		return off < len;
	}

	@Override
	public byte nextByte() {
		int ret;
		try {
			ret = ins.read();
		} catch(Exception e) {
			throw new IllegalStateException(e);
		}
		if(ret == -1) { throw new IllegalStateException("Past EOF!"); }
		off++;
		return (byte)ret;
	}

	@Override
	public long bytesLeft() {
		return len - off;
	}

}
