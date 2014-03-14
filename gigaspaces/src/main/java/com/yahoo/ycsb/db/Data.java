package com.yahoo.ycsb.db;

import java.util.Map;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceStorageType;
import com.gigaspaces.metadata.StorageType;

public class Data {

	private String id;
    private Map<String, byte[]> data;
    
    @SpaceId (autoGenerate=false)
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}

	@SpaceStorageType(storageType=StorageType.OBJECT)
	public Map<String, byte[]> getData() {
		return data;
	}
	public void setData(Map<String, byte[]> data) {
		this.data = data;
	}
}
