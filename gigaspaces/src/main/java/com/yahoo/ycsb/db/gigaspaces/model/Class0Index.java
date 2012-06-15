package com.yahoo.ycsb.db.gigaspaces.model;

import java.io.Serializable;

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;

@SpaceClass
public class Class0Index implements Serializable {
	String 	attrib1;
	Integer attrib2;
	Long 	attrib3;
	Double 	attrib4;
	String 	id;
	
	public String getAttrib1() {
		return attrib1;
	}
	public void setAttrib1(String attrib1) {
		this.attrib1 = attrib1;
	}
	public Integer getAttrib2() {
		return attrib2;
	}
	public void setAttrib2(Integer attrib2) {
		this.attrib2 = attrib2;
	}
	public Long getAttrib3() {
		return attrib3;
	}
	public void setAttrib3(Long attrib3) {
		this.attrib3 = attrib3;
	}
	public Double getAttrib4() {
		return attrib4;
	}
	public void setAttrib4(Double attrib4) {
		this.attrib4 = attrib4;
	}
	
	@SpaceId (autoGenerate=false)
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
}
