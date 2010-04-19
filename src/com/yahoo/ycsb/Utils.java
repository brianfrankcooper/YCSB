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

public class Utils
{
	static Random random=new Random();
	
	public static String ASCIIString(int length)
	{
		int interval='~'-' '+1;

		StringBuilder str=new StringBuilder();
		for (int i=0; i<length; i++)
		{
			char c=(char)(random.nextInt(interval)+' ');
			str.append(c);
		}

		return str.toString();
	}
	
	public static int hash(int val)
	{
		//return JenkinsHash(val);
		return FNVhash32(val);
	}

	public static int JenkinsHash(int val)
	{
		//from http://en.wikipedia.org/wiki/Jenkins_hash_function
		int hash = 0;

		for (int i = 0; i < 4; i++) {
			int octet=val&0x00ff;
			val=val>>8;
		
			hash += octet;
			hash += (hash << 10);
			hash ^= (hash >> 6);
		}
		hash += (hash << 3);
		hash ^= (hash >> 11);
		hash += (hash << 15);
		return Math.abs(hash);
	}
	
	public static final int FNV_offset_basis_32=0x811c9dc5;
	public static final int FNV_prime_32=16777619;
	
	/**
	 * 32 bit FNV hash. Produces more "random" hashes than (say) String.hashCode().
	 * 
	 * @param val The value to hash.
	 * @return
	 */
	public static int FNVhash32(int val)
	{
		//from http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
		int hashval = FNV_offset_basis_32;

		for (int i=0; i<4; i++)
		{
			int octet=val&0x00ff;
			val=val>>8;

			hashval = hashval ^ octet;
			hashval = hashval * FNV_prime_32;
			//hashval = hashval ^ octet;
		}
		return Math.abs(hashval);
	}

	public static final long FNV_offset_basis_64=0xCBF29CE484222325L;
	public static final long FNV_prime_64=1099511628211L;
	
	/**
	 * 64 bit FNV hash. Produces more "random" hashes than (say) String.hashCode().
	 * 
	 * @param val The value to hash.
	 * @return
	 */
	public static long FNVhash64(long val)
	{
		//from http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
		long hashval = FNV_offset_basis_64;

		for (int i=0; i<8; i++)
		{
			long octet=val&0x00ff;
			val=val>>8;

			hashval = hashval ^ octet;
			hashval = hashval * FNV_prime_64;
			//hashval = hashval ^ octet;
		}
		return Math.abs(hashval);
	}
}
