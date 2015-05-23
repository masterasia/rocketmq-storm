package com.alibaba.rocketmq.storm.hbase;

public enum HBaseClientStatus {
    JUST_CREATED,
    STARTING,
    START_FAILED,
    STARTED,
    TERMINATED
}
