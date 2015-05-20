package com.alibaba.rocketmq.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.fastjson.JSON;
import com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.storm.model.CRLog;
import com.alibaba.rocketmq.storm.redis.CacheManager;
import com.alibaba.rocketmq.storm.redis.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Li Zhanhui
 */
public class CRAggregationBolt implements IRichBolt, Constant {
    private static final long serialVersionUID = 7591260982890048043L;

    private static final Logger LOG = LoggerFactory.getLogger(CRAggregationBolt.class);

    private OutputCollector collector;

    private transient ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * We use one worker, one executor and one task strategy.
     */
    private HashMap<String /* offer_id */, HashMap<String /* affiliate_id*/, HashMap<String /* event_code*/, Long>>> resultMap = new HashMap<>();

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;

        final ScheduledExecutorService executorService =
                new ScheduledThreadPoolExecutor(2, new ThreadFactoryImpl("PersistThread"),
                        new ScheduledThreadPoolExecutor.CallerRunsPolicy());

        executorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                executorService.schedule(new PersistTask(), 0, TimeUnit.MILLISECONDS);
            }
        }, PERIOD, PERIOD, TimeUnit.SECONDS);
    }

    @Override
    public void execute(Tuple input) {
        Object msgObj = input.getValue(0);
        Object msgStat = input.getValue(1);

        try {
            if (msgObj instanceof MessageExt) {
                MessageExt msg = (MessageExt) msgObj;
                CRLog logEntry = JSON.parseObject(new String(msg.getBody(), Charset.forName("UTF-8")), CRLog.class);

                lock.writeLock().lockInterruptibly();

                if (!resultMap.containsKey(logEntry.getOffer_id())) {
                    HashMap<String, HashMap<String, Long>> affMap = new HashMap<>();
                    HashMap<String, Long> eventMap = new HashMap<>();
                    eventMap.put(logEntry.getEvent_code(), BASE);
                    affMap.put(logEntry.getAffiliate_id(), eventMap);
                    resultMap.put(logEntry.getOffer_id(), affMap);
                } else {
                    HashMap<String, HashMap<String, Long>> affMap = resultMap.get(logEntry.getOffer_id());
                    if (affMap.containsKey(logEntry.getAffiliate_id())) {
                        HashMap<String, Long> eventMap = affMap.get(logEntry.getAffiliate_id());
                        String eventCode = logEntry.getEvent_code();
                        if (eventMap.containsKey(eventCode)) {
                            eventMap.put(eventCode, eventMap.get(eventCode) + INCREMENT);
                        } else {
                            eventMap.put(logEntry.getEvent_code(), BASE);
                        }
                    } else {
                        HashMap<String, Long> eventMap = new HashMap<>();
                        eventMap.put(logEntry.getEvent_code(), BASE);
                        affMap.put(logEntry.getAffiliate_id(), eventMap);
                    }
                }
            } else {
                LOG.error("The first value in tuple should be MessageExt object");
            }
            LOG.info("Messages:" + msgObj + "\n statistics:" + msgStat);
        } catch (Exception e) {
            collector.fail(input);
            return;
        } finally {
            lock.writeLock().unlock();
        }
        collector.ack(input);
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }


    class PersistTask implements Runnable {

        CacheManager cacheManager = CacheManager.getInstance();

        public PersistTask() {
        }

        @Override
        public void run() {

            HashMap<String /* offer_id */, HashMap<String /* affiliate_id*/, HashMap<String /* event_code*/, Long>>> map = resultMap;
            try {
                lock.writeLock().lockInterruptibly();
                resultMap = new HashMap<>();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                lock.writeLock().unlock();
            }

            //TODO persist map
            for (Map.Entry<String, HashMap<String, HashMap<String, Long>>> row : map.entrySet()) {
                String offerId = row.getKey();
                HashMap<String, HashMap<String, Long>> affMap = row.getValue();
                for (Map.Entry<String, HashMap<String, Long>> affRow : affMap.entrySet()) {
                    String affId = affRow.getKey();
                    HashMap<String, Long> eventMap = affRow.getValue();
                    String key = offerId + "_" + affId;
                    StringBuilder click = new StringBuilder();
                    click.append(key).append(": {");

                    StringBuilder conversion = new StringBuilder();
                    conversion.append(key).append(": {");

                    StringBuilder values = new StringBuilder();
                    for (Map.Entry<String, Long> eventRow : eventMap.entrySet()) {
                        String event = eventRow.getKey();
                        if (event.startsWith("C")) {
                            click.append(event).append(": ").append(eventRow.getValue()).append(", ");
                        } else {
                            conversion.append(event).append(": ").append(eventRow.getValue()).append(", ");
                        }

                        values.append(event).append(": ").append(eventRow.getValue()).append(", ");
                    }

                    LOG.info(click.substring(0, click.length() - 2) + "}");
                    LOG.info(conversion.substring(0, click.length() - 2) + "}");
                    cacheManager.setKeyLive(key + "_" + System.currentTimeMillis(), PERIOD * NUMBERS, values.substring(0, click.length() - 2));
                }

            }
        }
    }
}
