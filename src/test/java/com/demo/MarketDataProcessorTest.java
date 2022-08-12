package com.demo;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class MarketDataProcessorTest {

    @ParameterizedTest
    @CsvSource({"0,1", "50,1", "100,1", "150,2", "200,2", "500,5"})
    public void givenMultiSymbolUpdatesComeInOneSecond_thenPublishMethodShouldNotCallMoreThan100Times(
            int symbols, int expectedTime) throws InterruptedException {
        //prepare
        MarketDataProcessor mockProcessor = Mockito.spy(MarketDataProcessor.class);
        List<MarketData> publishedSymbols = new ArrayList<>();
        Mockito.doAnswer((Answer<Void>) invocation -> {
            publishedSymbols.add(invocation.getArgumentAt(0, MarketData.class));
            return null;
        }).when(mockProcessor).publishAggregatedMarketData(any());

        //action
        mockProcessor.startWorkerThread();
        feedData(mockProcessor, symbols);

        //assert
        if(symbols <= 100) {
            assertTrue(publishedSymbols.size() >= 0 && publishedSymbols.size() <= 100);
        }else {
            Thread.sleep(1000);
            assertTrue(publishedSymbols.size() > 100 && publishedSymbols.size() < symbols);
        }
        Thread.sleep(expectedTime * 1000);
        verify(mockProcessor, times(symbols)).publishAggregatedMarketData(any());
        mockProcessor.stopWorkerThread();
    }

    @Test
    public void givenMultiUpdatesForEachSymbolComeInOneSecond_thenEachSymbolShouldBeUpdatedOnlyOnceWithTheLatestData()
            throws InterruptedException {
        //prepare
        MarketDataProcessor mockProcessor = Mockito.spy(MarketDataProcessor.class);
        Map<String, Double> publishedSymbols = new HashMap<>();
        Mockito.doAnswer((Answer<Void>) invocation -> {
            MarketData marketData = invocation.getArgumentAt(0, MarketData.class);
            publishedSymbols.put(marketData.getSymbol(), marketData.getPrice());
            return null;
        }).when(mockProcessor).publishAggregatedMarketData(any());

        //action
        mockProcessor.startWorkerThread();
        mockProcessor.onMessage(MarketData.builder().symbol("S1").price(1.0).updateTime(LocalDateTime.now()).build());
        mockProcessor.onMessage(MarketData.builder().symbol("S1").price(2.0).updateTime(LocalDateTime.now()).build());
        mockProcessor.onMessage(MarketData.builder().symbol("S2").price(1.0).updateTime(LocalDateTime.now()).build());
        mockProcessor.onMessage(MarketData.builder().symbol("S3").price(1.0).updateTime(LocalDateTime.now()).build());
        mockProcessor.onMessage(MarketData.builder().symbol("S2").price(2.0).updateTime(LocalDateTime.now()).build());
        mockProcessor.onMessage(MarketData.builder().symbol("S2").price(3.0).updateTime(LocalDateTime.now()).build());
        mockProcessor.onMessage(MarketData.builder().symbol("S1").price(3.0).updateTime(LocalDateTime.now()).build());
        mockProcessor.onMessage(MarketData.builder().symbol("S3").price(2.0).updateTime(LocalDateTime.now()).build());
        mockProcessor.onMessage(MarketData.builder().symbol("S3").price(3.0).updateTime(LocalDateTime.now()).build());

        //assert
        Thread.sleep(2000);
        assertEquals(3.0, publishedSymbols.get("S1"));
        assertEquals(3.0, publishedSymbols.get("S2"));
        assertEquals(3.0, publishedSymbols.get("S3"));
        verify(mockProcessor, times(3)).publishAggregatedMarketData(any());
        mockProcessor.stopWorkerThread();
    }

    @Test
    public void givenMultiSymbolUpdatesCannotBeCompletedInOneSecond_thenTheLatestDataOnEachSymbolWillBePublishedLater()
            throws InterruptedException {
        //prepare
        MarketDataProcessor mockProcessor = Mockito.spy(MarketDataProcessor.class);
        Map<String, Double> publishedSymbols = new HashMap<>();
        Mockito.doAnswer((Answer<Void>) invocation -> {
            MarketData marketData = invocation.getArgumentAt(0, MarketData.class);
            publishedSymbols.put(marketData.getSymbol(), marketData.getPrice());
            return null;
        }).when(mockProcessor).publishAggregatedMarketData(any());

        //action
        mockProcessor.startWorkerThread();
        feedData(mockProcessor, 300);

        //assert
        Thread.sleep(3 * 1000);
        assertEquals(100.0, publishedSymbols.get("S100"));
        assertEquals(200.0, publishedSymbols.get("S200"));
        assertEquals(300.0, publishedSymbols.get("S300"));
        mockProcessor.stopWorkerThread();
    }

    private void feedData(MarketDataProcessor marketDataProcessor, int symbols) throws InterruptedException {
        for(int i = 1; i <= symbols; i++) {
            marketDataProcessor.onMessage(MarketData.builder().symbol("S" + i).price(i).updateTime(LocalDateTime.now()).build());
            Thread.sleep(1000/symbols);
        }
    }
}
