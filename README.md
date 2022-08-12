# MarketDataProcessor

This is a data throttle controller which receives real-time market data from a source and then publishes them to other applications.

The controller implements the following features:
- Ensure that the number of calls of publishAggregatedMarketData method for publishing messages does not exceed 100 times per second, where this period is a sliding window.
- Ensure that each symbol does not update more than once per sliding window.
- Ensure that each symbol always has the latest market data published.
- Ensure the latest market data on each symbol will be published.

## Solution approach
- Use Google Guava RateLimiter utility class to limit the number of requests per second to fulfill throttling control.
- Design publishQueue and symbolToBeUpdatedMap to publish data and cache intermediate update data respectively.
- Design two worker threads, MarketDataPublisher for publishing data and MarketDataUpdater for getting the latest data for each symbol and only updating once per second.
- Use mutex to control concurrent access between threads.
- Mock MarketDataProcessor and its publishAggregatedMarketData method in unit test.

## Run tests
- Execute `./mvn test`

## Supported JDK
- Support JDK 17 by default.
- Change Java version to other version and remove `--add-opens java.base/java.lang=ALL-UNNAMED` configuration from maven-surefire-plugin if run with JDK 8/9/11.