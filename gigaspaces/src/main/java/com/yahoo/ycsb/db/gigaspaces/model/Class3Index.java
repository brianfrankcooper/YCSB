package com.yahoo.ycsb.db.gigaspaces.model;

import com.gigaspaces.annotation.pojo.SpaceIndex;

public class Class3Index extends Class2Index{

	@SpaceIndex
	public Long getAttrib3() {
		return attrib3;
	}

}
