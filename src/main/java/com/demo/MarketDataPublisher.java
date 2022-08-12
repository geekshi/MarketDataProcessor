package com.demo;

import com.google.common.util.concurrent.RateLimiter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.map.LinkedMap;

import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class MarketDataPublisher implements Runnable {

    private RateLimiter rateLimiter;
    private AtomicBoolean keepRun;
    private LinkedMap<String, MarketData> publishQueue;

    private MarketDataProcessor marketDataProcessor;

    private Thread publisherThread;

    public MarketDataPublisher(MarketDataProcessor marketDataProcessor, LinkedMap<String, MarketData> publishQueue) {
        this.marketDataProcessor = marketDataProcessor;
        this.publishQueue = publishQueue;
        this.rateLimiter = RateLimiter.create(100);
        this.keepRun = new AtomicBoolean(true);
        this.publisherThread = new Thread(this);
    }

    @Override
    public void run() {
        while(keepRun.get()) {
            try {
                rateLimiter.acquire(1);
                synchronized (publishQueue) {
                    if(!publishQueue.isEmpty()) {
                        MarketData marketData = publishQueue.remove(publishQueue.lastKey());
                        marketDataProcessor.publishAggregatedMarketData(marketData);
                    }
                }
            }catch (Exception e) {
                log.error(e.getMessage());
            }
        }
    }

    public void start() {
        this.publisherThread.start();
        log.info("Publisher thread is started");
    }

    public void stop() {
        keepRun.set(false);
        log.info("Publisher thread is stopped");
    }
}
