package com.alibaba.rocketmq.storm.model;

import java.util.Map;

public class HBaseData {

    private String table;

    private String columnFamily;

    private String rowKey;

    private Map<String, byte[]> data;

    public HBaseData() {
    }

    public HBaseData(String table, String columnFamily, String rowKey, Map<String, byte[]> data) {
        this.table = table;
        this.columnFamily = columnFamily;
        this.rowKey = rowKey;
        this.data = data;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public void setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public Map<String, byte[]> getData() {
        return data;
    }

    public void setData(Map<String, byte[]> data) {
        this.data = data;
    }
}
