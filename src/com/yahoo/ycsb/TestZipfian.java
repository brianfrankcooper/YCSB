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

public class TestZipfian {

	public static void main(String[] args) {
		int itemcount=100000;
		int operationcount=10000000;
		
		IntegerGenerator gen=new ZipfianGenerator(itemcount);
		
		int[] counter=new int[itemcount];
		
		for (int i=0; i<itemcount; i++)
		{
			counter[i]=0;
		}
		
		for (int i=0; i<operationcount; i++)
		{
			counter[gen.nextInt()]++;
		}
		
		for (int i=0; i<itemcount; i++)
		{
			System.out.println(i+", "+counter[i]);
		}
		
		/*
		int maxcount=0;
		
		for (int i=0; i<itemcount; i++)
		{
			if (counter[i]>maxcount)
			{
				maxcount=counter[i];
			}
		}
		
		int[] buckets=new int[maxcount+1];
		
		for (int i=0; i<maxcount+1; i++)
		{
			buckets[i]=0;
		}
		
		for (int i=0; i<itemcount; i++)
		{
			buckets[counter[i]]++;
		}
		
		for (int i=0; i<maxcount+1; i++)
		{
			if (buckets[i]>0)
			{
				System.out.println(i+", "+buckets[i]);
			}
		}
		
		*/
	}

}
