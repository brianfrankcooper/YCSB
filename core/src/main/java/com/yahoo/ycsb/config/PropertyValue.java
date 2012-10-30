package com.yahoo.ycsb.config;

public class PropertyValue {

    protected Object defaultValue;
    protected boolean required;

    public PropertyValue(Object defaultValue) {
        this.defaultValue = defaultValue;
    }

    public PropertyValue(boolean required) {
        this.required = required;
    }

    public PropertyValue(Object defaultValue, boolean required) {
        this.defaultValue = defaultValue;
        this.required = required;
    }
}
