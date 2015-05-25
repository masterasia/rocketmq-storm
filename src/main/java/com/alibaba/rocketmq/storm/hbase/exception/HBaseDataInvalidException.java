package com.alibaba.rocketmq.storm.hbase.exception;

public class HBaseDataInvalidException extends Exception {
    public HBaseDataInvalidException() {
        super();
    }

    public HBaseDataInvalidException(String message) {
        super(message);
    }

    public HBaseDataInvalidException(String message, Throwable cause) {
        super(message, cause);
    }

    public HBaseDataInvalidException(Throwable cause) {
        super(cause);
    }

    protected HBaseDataInvalidException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
