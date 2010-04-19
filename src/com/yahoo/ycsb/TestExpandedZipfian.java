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

public class TestExpandedZipfian {

	public static void main(String[] args) {

		int itemcount=10000000;
		int scalefactor=10;
		int opcount=itemcount*100;
		
		System.err.println("Initializing...");
		ZipfianGenerator gen=new ZipfianGenerator(itemcount*scalefactor);
		System.err.println("Running...");
		
		int[] count=new int[itemcount];
		for (int i=0; i<itemcount; i++)
		{
			count[i]=0;
		}
		
		for (int i=0; i<opcount; i++)
		{
			if (i%1000000==0)
			{
				System.err.println("  "+i+" operations");
			}
			int pick=Utils.hash(gen.nextInt())%itemcount;
			count[pick]++;
		}
		
		int hits=0;
		
		for (int i=0; i<itemcount; i++)
		{
			if (count[i]>0)
			{
				hits++;
			}
			System.out.println(count[i]+", "+i);
		}
		
		System.err.println(hits+", "+(100.0*((double)hits)/((double)itemcount))+" %");
		
	}

}
