package com.yahoo.ycsb.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@SuppressWarnings({"unchecked"})
public class PropertiesConfig {

    protected Properties properties = new Properties();
    private Map<String, PropertyValue> declared = new HashMap<String, PropertyValue>();

    public PropertiesConfig(Properties properties) {
        this.properties = properties;
    }

    public PropertiesConfig declareProperty(String key, Object defaultValue) {
        return declareProperty(key, new PropertyValue(defaultValue));
    }

    public PropertiesConfig declareProperty(String key, boolean required) {
        return declareProperty(key, new PropertyValue(required));
    }

    public PropertiesConfig declareProperty(String key, Object defaultValue, boolean required) {
        return declareProperty(key, new PropertyValue(defaultValue, required));
    }

    public PropertiesConfig declareProperty(String key, PropertyValue value) {
        declared.put(key, value);
        return this;
    }

    protected <T> T getDefaultValue(String key) {
        PropertyValue value = declared.get(key);
        if (value != null) {
            if (value.defaultValue != null) {
                return (T) value.defaultValue;
            } else if (value.required) {
                throw new PropertyRequiredException(String.format("Property required %1$s", key));
            }
        }
        return null;
    }

    public Boolean getBoolean(String key) {
        String property = properties.getProperty(key);
        if (property != null) {
            return Boolean.parseBoolean(property);
        } else {
            return getDefaultValue(key);
        }
    }

    public Integer getInteger(String key) {
        String property = properties.getProperty(key);
        if (property != null) {
            return Integer.valueOf(property);
        } else {
            return getDefaultValue(key);
        }
    }

    public Long getLong(String key) {
        String property = properties.getProperty(key);
        if (property != null) {
            return Long.valueOf(property);
        } else {
            return getDefaultValue(key);
        }
    }
}
