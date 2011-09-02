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

public class CellAccessControlList extends AccessControlList {
	private String rowname;
	private String columnname;

	public CellAccessControlList(String tablename, String rowname, String columnname, Set<String> entries, Permission permission) {
		init(tablename, rowname, columnname, entries, permission);
	}
	
	public CellAccessControlList(String tablename, String rowname, String columnname, Set<String> entries, Permission permission, boolean addRow, boolean addColumn) {
		if(addRow || addColumn) {
			entries = new HashSet<String>(entries);
			if(addRow) {
				entries.add(rowname);
			}
			if(addColumn) {
				entries.add(columnname);
			}
		}
		init(tablename, rowname, columnname, entries, permission);
	}

	private void init(String tablename, String rowname, String columnname, Set<String> entries, Permission permission) {
		this.setTablename(tablename);
		this.setRowname(rowname);
		this.setColumnname(columnname);
		this.setEntries(entries);
		this.setPermission(permission);
	}

	public String getRowname() {
		return rowname;
	}

	public void setRowname(String rowname) {
		this.rowname = rowname;
	}

	public String getColumnname() {
		return columnname;
	}

	public void setColumnname(String columnname) {
		this.columnname = columnname;
	}
}
