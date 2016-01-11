package com.company;

/**
 * Created by yangg on 2016/1/11.
 */
public class JsonDataMD5CheckException extends Exception {
    public JsonDataMD5CheckException() {
    }

    public JsonDataMD5CheckException(String message) {
        super(message);
    }

    public JsonDataMD5CheckException(String message, Throwable cause) {
        super(message, cause);
    }

    public JsonDataMD5CheckException(Throwable cause) {
        super(cause);
    }

    public JsonDataMD5CheckException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
