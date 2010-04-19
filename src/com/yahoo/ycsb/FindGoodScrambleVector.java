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

public class FindGoodScrambleVector {
static Random r=new Random();
	
	static final int permutations=2;
	
	static final int scrambles=3;
	
	static int[] scramblevector=null;
	static int[] bestscramblevector=null;
	static int bestcount=0;
	
	public static int[] scrambleArray()
	{
		int[] arr=new int[256];
		
		for (int i=0; i<256; i++)
		{
			arr[i]=i;
		}
	
		for (int p=0; p<permutations; p++)
		{
			for (int i=255; i>=1; i--)
			{
				int index=r.nextInt(256);
				int swp=arr[i];
				arr[i]=arr[index];
				arr[index]=swp;
			}
		}
		
		return arr;
	}
	
	public static int scramble(int val)
	{
		int ret=0;
		
		for (int i = 3; i >=0; i--) 
		{
			int tval=val>>(8*i);
			int octet=tval&0x00ff;
			
		
			ret<<=8;
			ret+=scramblevector[octet];
		}
		
		return ret;
	}
	
	public static int hash(int val, int itemcount)
	{
		//return Math.abs(Utils.scramble(Utils.scramble(val)))%itemcount;
		//return Math.abs(Utils.scramble(Utils.scramble(val)))%itemcount;
		int ret=val;
		
		for (int i=0; i<scrambles; i++)
		{
			ret=Math.abs(scramble(ret));
			//ret=scramble(ret);
		}
		
		//ret=Math.abs(ret);
		
		return ret%itemcount;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int items=100000;
	
		int iterations=0;
		
		while (true)
		{
			scramblevector=scrambleArray();
			
			//one iteration
			int[] count=new int[items];
			for (int i=0; i<items; i++)
			{
				count[i]=0;
			}

			for (int i=0; i<items; i++)
			{
				count[hash(i,items)]++;
			}

			int itemsleft=0;

			for (int i=0; i<items; i++)
			{
				if (count[i]>0)
				{
					itemsleft++;
				}
			}
			
			if (itemsleft>bestcount)
			{
				bestscramblevector=scramblevector;
				bestcount=itemsleft;
			}
		
			iterations++;
			
			if (iterations%1000==0)
			{
				System.out.println(iterations+". "+bestcount);
				System.out.print("     ");
				for (int i=0; i<256; i++)
				{
					System.out.print(bestscramblevector[i]+", ");
				}
				System.out.println();
				
			}
		}
		
		
	}
}
