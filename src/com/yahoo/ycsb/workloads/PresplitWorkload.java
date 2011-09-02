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

package com.yahoo.ycsb.workloads;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.DBFactory;
import com.yahoo.ycsb.UnknownDBException;
import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.UniformIntegerGenerator;
import com.yahoo.ycsb.measurements.Measurements;

/**
 * Some modifications to coreworklaod:
 * 1. support presplit operation by using property "split" with file name of the split keys
 * 2. for insert, instead of using sequential keys, users specify the distribution
 * 3. each key is fixed length (can be specified) : instead of user1, we use user000001
 *    So that user000002 is after user000001
 *    Otherwise user2 is not after user1, there are user10,user11,etc
 * 4. split by mulitple threads: assigned keys in round robin fasion.
 *    a. threadcount > 0
 *    b. need to call doinsert <==> load 
 *    c. each doinsert call from a thread will do the split assigned to it
 * @author linxiao
 *
 */
public class PresplitWorkload  extends CoreWorkload
{
	public static final String INSERT_START_PROPERTY="insertstart";
	public static final String INSERT_START_PROPERTY_DEFAULT="0";

	public static final String INSERT_END_PROPERTY="insertend";
	public static final String INSERT_END_PROPERTY_DEFAULT="0";

	public static final String SPLIT_PROPERTY="split";
	public static final String SPLIT_DEFAULT="no";
	public static final String SPLIT_ENABLED="yes";

	/**
	 *  The name of the property for the length of the integers in the key
	 */
	public static final String KEYINTEGERLENGTH_PROPERTY="keyintegerlength";

	/**
	 * Default number of integer length in the key
	 */
	public static final String KEYINTEGERLENGTH_PROPERTY_DEFAULT="20";
	protected static final String SPLITTOTAL_PROPERTY = "splittotal";
	protected static final String SPLITTOTAL_DEFAULT = "1";
	protected static final String SPLITFILE_PROPERTY = "splitfile";
	protected int keyintegerlength;
	protected String[] splitKeys = null;
	protected boolean threadSplit = false;
	protected int threadcount = 0;
	public int endKey;
	public int splittotal;
	protected int startKey;

	public void split(Properties p)
	{

		System.out.println("Split enabled...");
		/*
		 * For presplit only:
		 */
		if (p.getProperty(SPLITTOTAL_PROPERTY) != null) {
			// We're going to generate the splits 
			splittotal = Integer.parseInt(p.getProperty(SPLITTOTAL_PROPERTY, SPLITTOTAL_DEFAULT));
			long partitionSize = (endKey - startKey - 1) / (splittotal + 1) + 1;
			System.out.println("Paritition size would be:" + Long.toString(partitionSize));
			splitKeys = new String[splittotal];
			for (int i = 1; i <= splittotal; i ++)
			{
				splitKeys[i - 1] = "user" + String.format("%0"+keyintegerlength+"d", i * partitionSize + startKey); 
				System.out.println("Split point : "+splitKeys[i - 1]);
			}
		} else if (p.getProperty(SPLITFILE_PROPERTY) != null) {
			String splitFile = null;
			splitFile = p.getProperty(SPLITFILE_PROPERTY);

			try {

				BufferedReader in = new BufferedReader(new FileReader(splitFile));
				List<String> splitKeyList = new ArrayList<String>();
				try {
					while (in.ready()) {
						splitKeyList.add(in.readLine());
					}
					splitKeys = new String[splitKeyList.size()];
					splitKeyList.toArray(splitKeys);


				} catch (NumberFormatException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 



			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return;
			}
			// read splits from a file
		}
		else {
			System.out.println("Incorrect parameters for presplit...");
			return;
		}

		try {
			DB db = DBFactory.newDB(p.getProperty("db","com.yahoo.ycsb.BasicDB"),p);

			db.init();
			// Check whether we're going to split the table with multiple threads
			// or single call
			threadcount =Integer.parseInt(p.getProperty("threadcount", "0")); 
			 
				
			if ( threadcount == 0 ) {
				// split with the command here
				db.presplit(table, splitKeys);
			} else {
				threadSplit = true;
			}
		}catch (UnknownDBException e) {
			// TODO Auto-generated catch block 	
			e.printStackTrace();
		} catch (DBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void init(Properties p) throws WorkloadException
	{
		super.init(p);
		keyintegerlength = Integer.parseInt(p.getProperty(KEYINTEGERLENGTH_PROPERTY,KEYINTEGERLENGTH_PROPERTY_DEFAULT));

		startKey = Integer.parseInt(p.getProperty(INSERT_START_PROPERTY,INSERT_START_PROPERTY_DEFAULT));
		String requestdistrib=p.getProperty(REQUEST_DISTRIBUTION_PROPERTY,REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);
		endKey = Integer.parseInt(p.getProperty(INSERT_END_PROPERTY,INSERT_END_PROPERTY_DEFAULT));
		if (endKey < startKey)
			endKey = startKey;
		
		if (requestdistrib.compareTo("uniform") == 0)
		{
			System.out.println("The key chooser is uniform with range [" + startKey + ", " + endKey + ")");
			keychooser=new UniformIntegerGenerator(startKey, endKey);
		}

		//check whether we're doing presplit or no
		if (p.getProperty(SPLIT_PROPERTY, SPLIT_DEFAULT).compareTo(SPLIT_ENABLED) == 0)
		{
			split(p);
		} else {
			System.out.println("No presplit....");
		}
	}

	@Override
	public boolean doInsert(DB db, Object threadstate) {
		// TODO : We only check presplit by thread option in insert operation
		if (threadSplit) {
			int threadid = (Integer) threadstate;
			if (splitKeys.length <= threadid )
				return false;
			String[] tmpKeys = new String[(splitKeys.length - threadid) / threadcount + 1];
			int i, j = 0;
			for (i = threadid; i < splitKeys.length; i += threadcount) {
				tmpKeys[j] = splitKeys[i];
				j ++;
			}
			System.out.println("Splits from thread " + threadid);
			System.out.println(tmpKeys.toString());
			db.presplit(table, tmpKeys);
		} else {
			// Purely insert	
			doTransactionInsert(db);
		}
		return true;
	}

	public void doTransactionRead(DB db)
	{
		//choose a random key
		int keynum;
		// We may try to read rows which don't exist yet
		keynum=keychooser.nextInt();

		String keyname="user"+String.format("%0"+keyintegerlength+"d", keynum);

		HashSet<String> fields=null;

		if (!readallfields)
		{
			//read a random field  
			String fieldname="field"+fieldchooser.nextString();

			fields=new HashSet<String>();
			fields.add(fieldname);
		}

		db.read(table,keyname,fields,new HashMap<String,String>());
	}

	public void doTransactionReadModifyWrite(DB db)
	{
		//choose a random key
		int keynum;
		// We may try to readmodify write rows which don't exist yet.... ok, this doesn't make any sense at all.....
		// TODO : change it 
		keynum=keychooser.nextInt();
		String keyname="user"+String.format("%0"+keyintegerlength+"d", keynum);

		HashSet<String> fields=null;

		if (!readallfields)
		{
			//read a random field  
			String fieldname="field"+fieldchooser.nextString();

			fields=new HashSet<String>();
			fields.add(fieldname);
		}

		HashMap<String,String> values=new HashMap<String,String>();

		if (writeallfields)
		{
			//new data for all the fields
			for (int i=0; i<fieldcount; i++)
			{
				String fieldname="field"+i;
				String data=Utils.ASCIIString(fieldlength);		   
				values.put(fieldname,data);
			}
		}
		else
		{
			//update a random field
			String fieldname="field"+fieldchooser.nextString();
			String data=Utils.ASCIIString(fieldlength);		   
			values.put(fieldname,data);
		}

		//do the transaction

		long st=System.currentTimeMillis();

		db.read(table,keyname,fields,new HashMap<String,String>());

		db.update(table,keyname,values);

		long en=System.currentTimeMillis();

		Measurements.getMeasurements().measure("READ-MODIFY-WRITE", (int)(en-st));
	}

	public void doTransactionScan(DB db)
	{
		//choose a random key
		int keynum;
		// We may try to read rows which don't exist yet
		keynum=keychooser.nextInt();

		String startkeyname="user"+String.format("%0"+keyintegerlength+"d", keynum);

		//choose a random scan length
		int len=scanlength.nextInt();

		HashSet<String> fields=null;

		if (!readallfields)
		{
			//read a random field  
			String fieldname="field"+fieldchooser.nextString();

			fields=new HashSet<String>();
			fields.add(fieldname);
		}

		db.scan(table,startkeyname,len,fields,new Vector<HashMap<String,String>>());
	}

	public void doTransactionUpdate(DB db)
	{
		//choose a random key
		int keynum;
		// We may try to read rows which don't exist yet
		keynum=keychooser.nextInt();

		String keyname="user"+String.format("%0"+keyintegerlength+"d", keynum);

		HashMap<String,String> values=new HashMap<String,String>();

		if (writeallfields)
		{
			//new data for all the fields
			for (int i=0; i<fieldcount; i++)
			{
				String fieldname="field"+i;
				String data=Utils.ASCIIString(fieldlength);		   
				values.put(fieldname,data);
			}
		}
		else
		{
			//update a random field
			String fieldname="field"+fieldchooser.nextString();
			String data=Utils.ASCIIString(fieldlength);		   
			values.put(fieldname,data);
		}

		db.update(table,keyname,values);	
	}

	public void doTransactionInsert(DB db)
	{
		//choose a random key
		int keynum;
		// We may try to read rows which don't exist yet
		keynum=keychooser.nextInt();

		String keyname="user"+String.format("%0"+keyintegerlength+"d", keynum);

		HashMap<String,String> values=new HashMap<String,String>();
		for (int i=0; i<fieldcount; i++)
		{
			String fieldkey="field"+i;
			String data=Utils.ASCIIString(fieldlength);
			values.put(fieldkey,data);
		}
		db.insert(table,keyname,values);
	}

	/*
	 * Return the threadid for each thread to split table in parallel
	 *   
	 * @return 
	 */
	public Object initThread(Properties p, int mythreadid, int threadcount) throws WorkloadException
	{
		return mythreadid;
	} 
}
