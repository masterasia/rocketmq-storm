package com.alibaba.rocketmq.storm;

import com.alibaba.rocketmq.storm.redis.CacheManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by robert on 2015/5/20.
 */
public class TestCacheManager {
    private CacheManager cacheManager;

    @Before
    public void init() {
        cacheManager = CacheManager.getInstance();
    }

    @Test
    public void set() {
        Map<String, String> kValues = new HashMap<String, String>();
        kValues.put("testKey", "robert");
        kValues.put("1", "1");
        kValues.put("2", "2");
        kValues.put("3", "3");
        kValues.put("4", "4");
        cacheManager.append("4", "four");

        cacheManager.set(kValues);
        System.out.println(cacheManager.getValue("4"));

    }

    @Test
    public void calculateSize() {
        System.out.println(cacheManager.getValue("four"));
        System.out.println(cacheManager.calculateSize() + "");
    }

    @Test
    public void getValue() {
        String value = cacheManager.getValue("testKey");
        Assert.assertNotNull(value);
        Assert.assertEquals("robert", value);
    }

    @Test
    public void remove(){
        cacheManager.deleteValue("");
    }
}
