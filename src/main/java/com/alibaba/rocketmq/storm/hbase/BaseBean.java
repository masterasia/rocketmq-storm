package com.alibaba.rocketmq.storm.hbase;

/**
 * Created by Administrator on 2015/5/22.
 */
public class BaseBean {
    private String affId;
    private String offId;
    private String click;
    private String conversion;

    public String getAffId() {
        return affId;
    }

    public void setAffId(String affId) {
        this.affId = affId;
    }

    public String getOffId() {
        return offId;
    }

    public void setOffId(String offId) {
        this.offId = offId;
    }

    public String getClick() {
        return click;
    }

    public void setClick(String click) {
        this.click = click;
    }

    public String getConversion() {
        return conversion;
    }

    public void setConversion(String conversion) {
        this.conversion = conversion;
    }
}
