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


public class TestCollisions {

	static final int[] scramblevector={251, 37, 147, 110, 233, 38, 225, 244, 17, 31, 231, 33, 249, 93, 229, 177, 47, 142, 55, 65, 106, 104, 178, 61, 182, 89, 222, 51, 108, 165, 100, 111, 113, 195, 197, 102, 35, 88, 9, 184, 128, 239, 240, 90, 92, 98, 78, 158, 219, 16, 112, 121, 13, 56, 236, 58, 82, 73, 167, 216, 224, 223, 14, 39, 175, 70, 203, 83, 50, 238, 139, 125, 211, 12, 141, 63, 237, 152, 135, 144, 48, 138, 213, 21, 97, 53, 245, 172, 166, 114, 194, 94, 198, 127, 191, 36, 148, 67, 30, 4, 71, 27, 221, 173, 72, 143, 80, 202, 181, 160, 159, 86, 201, 188, 59, 105, 25, 11, 57, 156, 24, 149, 183, 241, 18, 161, 124, 234, 176, 41, 133, 136, 122, 218, 10, 210, 45, 77, 116, 42, 206, 120, 29, 115, 180, 235, 81, 7, 163, 205, 22, 187, 107, 199, 32, 8, 227, 129, 19, 5, 190, 130, 209, 212, 243, 146, 99, 119, 169, 60, 68, 254, 52, 40, 255, 134, 96, 79, 192, 189, 101, 20, 226, 164, 2, 217, 207, 3, 91, 137, 252, 247, 117, 15, 118, 109, 230, 170, 44, 66, 200, 140, 84, 154, 215, 62, 196, 157, 153, 123, 150, 174, 208, 248, 145, 95, 1, 85, 193, 103, 204, 131, 186, 49, 168, 253, 132, 220, 242, 214, 162, 228, 74, 179, 232, 34, 75, 151, 246, 69, 23, 6, 126, 76, 0, 185, 46, 87, 64, 54, 26, 43, 28, 155, 171, 250};
	static final int[] scramblevector2={35, 210, 27, 178, 218, 165, 145, 87, 10, 133, 134, 127, 92, 205, 96, 250, 136, 59, 84, 248, 19, 20, 147, 125, 89, 180, 246, 12, 137, 168, 142, 69, 130, 199, 150, 223, 0, 26, 14, 57, 155, 236, 204, 195, 66, 97, 56, 230, 159, 253, 229, 52, 182, 39, 189, 255, 44, 71, 139, 5, 3, 63, 121, 60, 135, 193, 8, 51, 49, 75, 76, 129, 6, 38, 33, 46, 80, 24, 103, 249, 251, 94, 32, 162, 161, 131, 225, 34, 149, 203, 211, 170, 93, 74, 109, 36, 45, 101, 124, 13, 65, 41, 128, 98, 148, 233, 112, 156, 85, 146, 81, 172, 219, 100, 240, 17, 18, 23, 202, 220, 239, 169, 68, 115, 143, 48, 47, 77, 58, 7, 241, 244, 173, 164, 151, 167, 70, 254, 181, 194, 191, 132, 227, 28, 158, 171, 30, 166, 72, 200, 64, 79, 163, 108, 104, 25, 42, 126, 153, 54, 11, 198, 119, 221, 187, 228, 120, 207, 122, 176, 222, 208, 16, 113, 102, 185, 116, 242, 86, 105, 234, 217, 157, 206, 154, 238, 174, 190, 209, 144, 37, 2, 9, 55, 15, 247, 83, 183, 188, 152, 50, 231, 43, 212, 184, 245, 232, 177, 214, 224, 111, 114, 243, 197, 235, 140, 21, 61, 29, 62, 40, 160, 186, 138, 118, 67, 196, 110, 237, 53, 82, 175, 107, 73, 213, 123, 95, 117, 192, 201, 215, 216, 78, 226, 91, 106, 141, 90, 4, 99, 252, 179, 88, 1, 22, 31};
	static final int[] scramblevector3={208, 11, 56, 253, 167, 174, 228, 75, 251, 5, 227, 198, 57, 120, 103, 169, 218, 76, 24, 45, 170, 83, 55, 139, 40, 20, 87, 192, 21, 188, 200, 63, 13, 143, 99, 144, 96, 6, 36, 244, 176, 175, 59, 118, 67, 155, 195, 121, 77, 220, 101, 60, 235, 163, 212, 116, 78, 153, 216, 23, 219, 8, 138, 126, 136, 47, 217, 85, 247, 34, 50, 7, 252, 221, 64, 209, 22, 206, 110, 157, 249, 193, 199, 236, 30, 25, 89, 245, 182, 124, 154, 242, 43, 141, 80, 62, 201, 4, 51, 16, 254, 1, 186, 191, 86, 137, 100, 106, 91, 10, 215, 223, 95, 164, 129, 71, 246, 239, 127, 190, 39, 93, 119, 151, 210, 184, 28, 224, 202, 123, 72, 108, 146, 12, 27, 204, 73, 90, 179, 66, 84, 237, 65, 173, 158, 37, 105, 194, 178, 159, 230, 148, 35, 42, 131, 140, 15, 94, 250, 48, 180, 203, 166, 0, 88, 49, 171, 29, 122, 205, 111, 185, 107, 160, 189, 14, 98, 69, 46, 3, 31, 168, 241, 26, 225, 145, 156, 234, 243, 135, 214, 54, 109, 70, 248, 233, 161, 17, 222, 196, 183, 115, 41, 232, 213, 9, 134, 211, 58, 181, 113, 172, 132, 150, 79, 255, 114, 165, 152, 112, 33, 97, 44, 207, 147, 177, 197, 61, 2, 149, 82, 162, 38, 231, 92, 32, 128, 226, 52, 238, 125, 117, 68, 102, 142, 74, 53, 240, 19, 18, 187, 104, 229, 130, 133, 81};
	
	static final int[][] scramblevectors={scramblevector,scramblevector2,scramblevector3};
	
	static final int scrambles=3;
	
	public static int scramble(int val)
	{
		int ret=0;
		
		for (int s=0; s<scrambles; s++)
		{
			for (int i = 3; i >=0; i--) 
			{
				int tval=val>>(8*i);
				int octet=tval&0x00ff;
			
		
				ret<<=8;
				ret+=scramblevectors[s][octet];
			}
		}
		
		return ret;
	}
	
	/*
	public static int hash(int val, int itemcount)
	{
		//return Math.abs(Utils.scramble(Utils.scramble(val)))%itemcount;
		//return Math.abs(Utils.scramble(Utils.scramble(val)))%itemcount;
		int ret=val;
		
		ret=Math.abs(Utils.scramble(ret));
		
		return ret%itemcount;
	}
	*/
	
	public static int a=787690635;
	public static int b=-1147376859;
	
	public static int hash(int val, int itemcount)
	{
		return Math.abs((val*a+b)%itemcount);
	}
	
	public static void main(String[] args)
	{
		Random r=new Random();
		for (int itemcount=1000000; itemcount<500000000; itemcount+=1000000)
		{
			int bestcount=0;
			int besta=0;
			int bestb=0;
			int iterations=0;
			while (true)
			{
				a=r.nextInt(itemcount);
				b=r.nextInt(itemcount);
				int count=testVector(itemcount);
				if (count>bestcount)
				{
					bestcount=count;
					besta=a;
					bestb=b;
				}
				iterations++;
				if (iterations%1000==0)
				{
					System.err.println("     "+itemcount+" - "+(100.0*((double)bestcount)/((double)itemcount))+"(a="+besta+", b="+bestb+")");
				}
				if (((double)bestcount)/((double)itemcount)>0.9)
				{
					break;
				}
			}
			System.err.println(itemcount+" - "+(100.0*((double)bestcount)/((double)itemcount))+"(a="+besta+", b="+bestb+")");
			System.out.println(itemcount+" - "+(100.0*((double)bestcount)/((double)itemcount))+"(a="+besta+", b="+bestb+")");
		}
	}
	
	public static int testVector(int itemcount)
	{
		int[] countarray=new int[itemcount];
		
		for (int i=0; i<itemcount; i++)
		{
			countarray[i]=0;
		}
			
		for (int i=0; i<itemcount; i++)
		{
			countarray[hash(i,itemcount)]++;
		}
		
		int count=0;
		for (int i=0; i<itemcount; i++)
		{
			if (countarray[i]>0)
			{
				count++;
			}
		}
		
		/*
		for (int i=0; i<itemcount; i++)
		{
			System.out.println(i+", "+hash(i,itemcount));
		}
		*/
		
		return count;
	}

}
