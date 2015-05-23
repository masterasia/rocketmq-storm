package com.alibaba.rocketmq.storm.hbase.exception;

public class HBasePersistenceException extends Exception {

    public HBasePersistenceException() {
        super();
    }

    public HBasePersistenceException(String message) {
        super(message);
    }

    public HBasePersistenceException(String message, Throwable cause) {
        super(message, cause);
    }

    public HBasePersistenceException(Throwable cause) {
        super(cause);
    }

    protected HBasePersistenceException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
