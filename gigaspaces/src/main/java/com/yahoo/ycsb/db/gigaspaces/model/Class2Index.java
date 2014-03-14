package com.yahoo.ycsb.db.gigaspaces.model;

import com.gigaspaces.annotation.pojo.SpaceIndex;

public class Class2Index extends Class1Index{

	@SpaceIndex
	public Integer getAttrib2() {
		return attrib2;
	}

}
