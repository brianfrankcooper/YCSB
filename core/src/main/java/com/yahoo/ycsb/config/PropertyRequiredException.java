package com.yahoo.ycsb.config;

public class PropertyRequiredException extends RuntimeException {

    public PropertyRequiredException(String message) {
        super(message);
    }

    public PropertyRequiredException(String message, Throwable cause) {
        super(message, cause);
    }

    public PropertyRequiredException(Throwable cause) {
        super(cause);
    }
}
