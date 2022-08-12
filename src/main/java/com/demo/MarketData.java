package com.demo;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@Builder
public class MarketData {

    private String symbol;
    private double price;
    private LocalDateTime updateTime;
}
