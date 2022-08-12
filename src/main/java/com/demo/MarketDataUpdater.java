package com.demo;

import com.google.common.util.concurrent.RateLimiter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.map.LinkedMap;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class MarketDataUpdater implements Runnable {

    private Map<String, RateLimiter> symbolUpdateRateLimiterMap;

    private Map<String, List<MarketData>> symbolToBeUpdatedMap;

    private LinkedMap<String, MarketData> publishQueue;

    private AtomicBoolean keepRun;

    private Thread updaterThread;

    public MarketDataUpdater(Map<String, RateLimiter> symbolUpdateRateLimiterMap,
                             Map<String, List<MarketData>> symbolToBeUpdatedMap,
                             LinkedMap<String, MarketData> publishQueue) {
        this.symbolUpdateRateLimiterMap = symbolUpdateRateLimiterMap;
        this.symbolToBeUpdatedMap = symbolToBeUpdatedMap;
        this.publishQueue = publishQueue;
        this.keepRun = new AtomicBoolean(true);
        this.updaterThread = new Thread(this);
    }

    @Override
    public void run() {
        while(keepRun.get()) {
            try {
                symbolToBeUpdatedMap.forEach((k, v) -> {
                    symbolUpdateRateLimiterMap.get(k).acquire(1);
                    if(!v.isEmpty()) {
                        publishQueue.put(k, v.get(v.size() - 1));
                        symbolToBeUpdatedMap.remove(k);
                    }
                });
            }catch (Exception e) {
                log.error(e.getMessage());
            }
        }
    }

    public void start() {
        this.updaterThread.start();
        log.info("Updater thread is started");
    }

    public void stop() {
        keepRun.set(false);
        log.info("Updater thread is stopped");
    }
}
