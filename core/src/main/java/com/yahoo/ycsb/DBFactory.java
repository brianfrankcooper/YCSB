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

import java.util.Properties;

/**
 * Creates a DB layer by dynamically classloading the specified DB class.
 */
public class DBFactory
{
      @SuppressWarnings("unchecked")
	public static DB newDB(String dbname, Properties properties) throws UnknownDBException
      {
	 ClassLoader classLoader = DBFactory.class.getClassLoader();

	 DB ret=null;

	 try 
	 {
	    Class dbclass = classLoader.loadClass(dbname);
	    //System.out.println("dbclass.getName() = " + dbclass.getName());
	    
	    ret=(DB)dbclass.newInstance();
	 }
	 catch (Exception e) 
	 {  
	    e.printStackTrace();
	    return null;
	 }
	 
	 ret.setProperties(properties);

	 return new DBWrapper(ret);
      }
      
}
