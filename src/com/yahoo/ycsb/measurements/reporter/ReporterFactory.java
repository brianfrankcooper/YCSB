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

package com.yahoo.ycsb.measurements.reporter;

import java.util.Properties;

public class ReporterFactory
{
      @SuppressWarnings("unchecked")
      public static Reporter newReporter(String reporterName, Properties properties)
      {
	 ClassLoader classLoader = ReporterFactory.class.getClassLoader();

	 Reporter ret=null;

	 try 
	 {
	    Class reporterclass = classLoader.loadClass(reporterName);
	    ret=(Reporter) reporterclass.newInstance();
	 }
	 catch (Exception e) 
	 {  
	    e.printStackTrace();
	    return null;
	 }
	 
	 ret.setProperties(properties);

	 return ret;
      }
}
