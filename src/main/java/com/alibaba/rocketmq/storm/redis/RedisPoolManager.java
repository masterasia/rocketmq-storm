package com.alibaba.rocketmq.storm.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.ResourceBundle;

/**
 * <p>
 *     Redis pool manager to better handle life cycle of {@link Jedis} client instances.
 * </p>
 *
 * @author Xu tao
 * @version 1.0
 * @since 1.0
 */
public class RedisPoolManager {


    private static JedisPool pool;

    static {
        ResourceBundle bundle = ResourceBundle.getBundle("redis");
        if (bundle == null)
            throw new IllegalArgumentException("[redis.properties] is not found");

        JedisPoolConfig config = new JedisPoolConfig();
//        config.setMaxActive(Integer.valueOf(bundle.getString("redis.pool.maxActive")));
        config.setMaxIdle(Integer.valueOf(bundle.getString("redis.pool.maxIdle")));
        config.setMaxWaitMillis(Long.valueOf(bundle.getString("redis.pool.maxWait")));
        config.setTestOnBorrow(Boolean.valueOf(bundle.getString("redis.pool.testOnBorrow")));
        config.setTestOnReturn(Boolean.valueOf(bundle.getString("redis.pool.testOnReturn")));

        pool = new JedisPool(config, bundle.getString("redis.ip"), Integer.valueOf(bundle.getString("redis.port")), 120);
    }

    /**
     * Get Jedis resource from the pool
     * @return
     */
    public static Jedis createInstance() {
        Jedis jedis = pool.getResource();

        return jedis;
    }

    /**
     * Return the resource to pool
     * @param jedis
     */
    public static void returnResource(Jedis jedis) {
        pool.returnResource(jedis);
    }

}
