package com.yahoo.ycsb.db.gigaspaces.model;

import com.gigaspaces.annotation.pojo.SpaceIndex;

public class Class1Index extends Class0Index {

	@SpaceIndex
	public String getAttrib1() {
		return attrib1;
	}
}
