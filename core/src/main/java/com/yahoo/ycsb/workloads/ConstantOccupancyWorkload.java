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
package com.yahoo.ycsb.workloads;

import java.util.Properties;

import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.generator.IntegerGenerator;

/**
 * A disk-fragmenting workload.
 * <p>
 * Properties to control the client:
 * </p>
 * <UL>
 * <LI><b>disksize</b>: how many bytes of storage can the disk store? (default 100,000,000)
 * <LI><b>occupancy</b>: what fraction of the available storage should be used? (default 0.9)
 * <LI><b>requestdistribution</b>: what distribution should be used to select the records to operate on - uniform, zipfian or latest (default: histogram)
 * </ul> 
 *
 *
 * <p> See also:
 * Russell Sears, Catharine van Ingen.
 * <a href='https://database.cs.wisc.edu/cidr/cidr2007/papers/cidr07p34.pdf'>Fragmentation in Large Object Repositories</a>,
 * CIDR 2006. [<a href='https://database.cs.wisc.edu/cidr/cidr2007/slides/p34-sears.ppt'>Presentation</a>]
 * </p>
 *
 *
 * @author sears
 *
 */
public class ConstantOccupancyWorkload extends CoreWorkload {
	long disksize;
	long storageages;
	IntegerGenerator objectsizes;
	double occupancy;
	
	long object_count;
	
	public static final String STORAGE_AGE_PROPERTY = "storageages";
	public static final long   STORAGE_AGE_PROPERTY_DEFAULT = 10;
	
	public static final String DISK_SIZE_PROPERTY = "disksize";
	public static final long   DISK_SIZE_PROPERTY_DEFAULT = 100 * 1000 * 1000;
	
	public static final String OCCUPANCY_PROPERTY = "occupancy";
	public static final double OCCUPANCY_PROPERTY_DEFAULT = 0.9;
	
	@Override
	public void init(Properties p) throws WorkloadException
	{
		disksize    = Long.parseLong(    p.getProperty(DISK_SIZE_PROPERTY, DISK_SIZE_PROPERTY_DEFAULT+""));
		storageages = Long.parseLong(    p.getProperty(STORAGE_AGE_PROPERTY, STORAGE_AGE_PROPERTY_DEFAULT+""));
		occupancy   = Double.parseDouble(p.getProperty(OCCUPANCY_PROPERTY, OCCUPANCY_PROPERTY_DEFAULT+""));
		
		if(p.getProperty(Client.RECORD_COUNT_PROPERTY) != null ||
		   p.getProperty(Client.INSERT_COUNT_PROPERTY) != null ||
		   p.getProperty(Client.OPERATION_COUNT_PROPERTY) != null) {
			System.err.println("Warning: record, insert or operation count was set prior to initting ConstantOccupancyWorkload.  Overriding old values.");
		}
		IntegerGenerator g = CoreWorkload.getFieldLengthGenerator(p);
		double fieldsize = g.mean();
		int fieldcount = Integer.parseInt(p.getProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_PROPERTY_DEFAULT));

		object_count = (long)(occupancy * ((double)disksize / (fieldsize * (double)fieldcount)));
                if(object_count == 0) {
                    throw new IllegalStateException("Object count was zero.  Perhaps disksize is too low?");
                }
		p.setProperty(Client.RECORD_COUNT_PROPERTY, object_count+"");
		p.setProperty(Client.OPERATION_COUNT_PROPERTY, (storageages*object_count)+"");
		p.setProperty(Client.INSERT_COUNT_PROPERTY, object_count+"");

		super.init(p);
	}

}
