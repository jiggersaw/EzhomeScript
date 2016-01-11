package com.company;

/**
 * Created by yangg on 2016/1/11.
 */
public class JsonParseUpdateException extends Exception {
    public JsonParseUpdateException() {
    }

    public JsonParseUpdateException(String message) {
        super(message);
    }

    public JsonParseUpdateException(String message, Throwable cause) {
        super(message, cause);
    }

    public JsonParseUpdateException(Throwable cause) {
        super(cause);
    }

    public JsonParseUpdateException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
