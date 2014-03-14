package com.yahoo.ycsb.db.gigaspaces.model;

import com.gigaspaces.annotation.pojo.SpaceIndex;

public class Class4Index extends Class3Index{

	@SpaceIndex
	public Double getAttrib4() {
		return attrib4;
	}
}
