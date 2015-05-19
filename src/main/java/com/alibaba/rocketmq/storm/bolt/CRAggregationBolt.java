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
public class CRAggregationBolt implements IRichBolt {
    private static final long   serialVersionUID = 7591260982890048043L;

    private static final Logger LOG              = LoggerFactory.getLogger(CRAggregationBolt.class);

    private OutputCollector     collector;

    private transient ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * We use one worker, one executor and one task strategy.
     */
    private HashMap<String /* offer_id */, HashMap<String /* affiliate_id*/, HashMap<String /* event_code*/, Long>>> resultMap = new HashMap<>();

    private transient ScheduledExecutorService executorService =
            new ScheduledThreadPoolExecutor(2, new ThreadFactoryImpl("PersistThread"),
                    new ScheduledThreadPoolExecutor.CallerRunsPolicy());

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;

        executorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                executorService.schedule(new PersistTask(), 0, TimeUnit.MILLISECONDS);
            }
        }, 30, 30, TimeUnit.SECONDS);
    }

    @Override
    public void execute(Tuple input) {
        Object msgObj = input.getValue(0);
        Object msgStat = input.getValue(1);

        try {
            if (msgObj instanceof MessageExt) {
                MessageExt msg = (MessageExt)msgObj;
                CRLog logEntry = JSON.parseObject(new String(msg.getBody(), Charset.forName("UTF-8")), CRLog.class);

                lock.writeLock().lockInterruptibly();

                if (!resultMap.containsKey(logEntry.getOffer_id())) {
                    HashMap<String, HashMap<String, Long>> affMap = new HashMap<>();
                    HashMap<String, Long> eventMap = new HashMap<>();
                    eventMap.put(logEntry.getEvent_code(), 1L);
                    affMap.put(logEntry.getAffiliate_id(), eventMap);
                    resultMap.put(logEntry.getOffer_id(), affMap);
                } else {
                    HashMap<String, HashMap<String, Long>> affMap = resultMap.get(logEntry.getOffer_id());
                    if (affMap.containsKey(logEntry.getAffiliate_id())) {
                        HashMap<String, Long> eventMap = affMap.get(logEntry.getAffiliate_id());
                        String eventCode = logEntry.getEvent_code();
                        if (eventMap.containsKey(eventCode)) {
                            eventMap.put(eventCode, eventMap.get(eventCode) + 1);
                        } else {
                            eventMap.put(logEntry.getEvent_code(), 1L);
                        }
                    } else {
                        HashMap<String, Long> eventMap = new HashMap<>();
                        eventMap.put(logEntry.getEvent_code(), 1L);
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

                    StringBuilder click = new StringBuilder();
                    click.append(offerId).append("_").append(affId).append(": {");

                    StringBuilder conversion = new StringBuilder();
                    conversion.append(offerId).append("_").append(affId).append(": {");

                    for (Map.Entry<String, Long> eventRow : eventMap.entrySet()) {
                        String event = eventRow.getKey();
                        if (event.startsWith("C")) {
                            click.append(event).append(": ").append(eventRow.getValue()).append(", ");
                        } else {
                            conversion.append(event).append(": ").append(eventRow.getValue()).append(", ");
                        }
                    }

                    LOG.info(click.substring(0, click.length() - 2) + "}");
                    LOG.info(conversion.substring(0, click.length() - 2) + "}");
                }

            }
        }
    }
}
