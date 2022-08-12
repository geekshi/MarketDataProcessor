package com.demo;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.collections4.map.LinkedMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MarketDataProcessor {

    private LinkedMap<String, MarketData> publishQueue = new LinkedMap<>();

    private Map<String, RateLimiter> symbolUpdateRateLimiterMap = new ConcurrentHashMap<>();

    private Map<String, List<MarketData>> symbolToBeUpdatedMap = new ConcurrentHashMap<>();

    private Map<String, String> symbolMutexMap = new ConcurrentHashMap<String, String>();

    private MarketDataPublisher marketDataPublisher;

    private MarketDataUpdater marketDataUpdater;

    public MarketDataProcessor() {
        marketDataPublisher = new MarketDataPublisher(this, publishQueue);
        marketDataUpdater = new MarketDataUpdater(symbolUpdateRateLimiterMap, symbolToBeUpdatedMap, publishQueue);
    }

    public void onMessage(MarketData data) {
        String symbol = data.getSymbol();
        symbolUpdateRateLimiterMap.putIfAbsent(symbol, RateLimiter.create(1));
        symbolMutexMap.putIfAbsent(symbol, symbol);
        synchronized(symbolMutexMap.get(symbol)) {
            symbolToBeUpdatedMap.putIfAbsent(symbol, new ArrayList<>());
            symbolToBeUpdatedMap.computeIfPresent(symbol, (k, v) -> {
                v.add(data);
                return v;
            });
        }
    }

    public void publishAggregatedMarketData(MarketData data) {
        // Do Nothing, assume implemented.
    }

    public void startWorkerThread() {
        marketDataPublisher.start();
        marketDataUpdater.start();
    }

    public void stopWorkerThread() {
        marketDataUpdater.stop();
        marketDataPublisher.stop();
    }
}
