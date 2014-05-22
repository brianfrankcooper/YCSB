package com.yahoo.ycsb;

import java.util.HashMap;

public class ThreadState {
    private String key;
    private HashMap<String, ByteIterator> values;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public HashMap<String, ByteIterator> getValues() {
        return values;
    }

    public void setValues(HashMap<String, ByteIterator> values) {
        this.values = values;
    }
}
