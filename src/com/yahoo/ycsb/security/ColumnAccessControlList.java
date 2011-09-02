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

package com.yahoo.ycsb.security;

import java.util.HashSet;
import java.util.Set;

public class ColumnAccessControlList extends AccessControlList {
	private String columnname;

	public ColumnAccessControlList(String tablename, String columnname, Set<String> entries, Permission permission) {
		init(tablename, columnname, entries, permission);
	}

	public ColumnAccessControlList(String tablename, String columnname, Set<String> entries, Permission permission, boolean addColumn) {
		if(addColumn) {
			entries = new HashSet<String>(entries);
			entries.add(columnname);
		}
		init(tablename, columnname, entries, permission);
	}

	public void init(String tablename, String columnname, Set<String> entries, Permission permission) {
		this.setTablename(tablename);
		this.setColumnname(columnname);
		this.setEntries(entries);
		this.setPermission(permission);
	}

	public String getColumnname() {
		return columnname;
	}

	public void setColumnname(String columnname) {
		this.columnname = columnname;
	}
}
