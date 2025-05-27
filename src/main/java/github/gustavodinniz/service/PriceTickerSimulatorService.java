package github.gustavodinniz.service;

import github.gustavodinniz.config.AppProperties;
import github.gustavodinniz.model.PriceTick;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class PriceTickerSimulatorService {

    private final AppProperties appProperties; // Injetar configurações
    private final Random random = new Random();

    private final Sinks.Many<PriceTick> priceTickSink = Sinks.many().multicast().onBackpressureBuffer();

    private final Map<String, BigDecimal> currentPrices = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        if (appProperties.getPriceSimulator().isEnabled()) {
            log.info("Initializing Price Ticker Simulator...");
            List<String> symbols = Arrays.asList(appProperties.getTicker().getSymbolsToMonitor().split(","));
            Map<String, BigDecimal> basePrices = appProperties.getPriceSimulator().getBasePrices();

            symbols.forEach(symbol ->
                    currentPrices.put(symbol, basePrices.getOrDefault(symbol, BigDecimal.valueOf(100.00 + random.nextDouble() * 10)))
            );

            Flux.interval(Duration.ofMillis(appProperties.getPriceSimulator().getIntervalMs()))
                    .flatMap(tick -> Flux.fromIterable(symbols)
                            .map(this::generateRandomPriceTick)
                    )
                    .doOnNext(priceTickSink::tryEmitNext)
                    .doOnError(error -> log.error("Error in price ticker simulator flux: ", error))
                    .subscribe(
                            data -> log.trace("Simulated tick: {}", data),
                            error -> log.error("Price Ticker Simulator stopped due to error: ", error),
                            () -> log.info("Price Ticker Simulator completed (should not happen with interval).")
                    );
            log.info("Price Ticker Simulator started for symbols: {}", symbols);
        } else {
            log.info("Price Ticker Simulator is disabled.");
        }
    }

    private PriceTick generateRandomPriceTick(String symbol) {
        BigDecimal lastPrice = currentPrices.get(symbol);
        double percentageChange = (random.nextDouble() - 0.5) * 2 *
                (appProperties.getPriceSimulator().getPriceVariationPercentage() / 100.0);

        BigDecimal priceChange = lastPrice.multiply(BigDecimal.valueOf(percentageChange));
        BigDecimal newPrice = lastPrice.add(priceChange).setScale(2, RoundingMode.HALF_UP);


        if (newPrice.signum() < 0) {
            newPrice = lastPrice.multiply(BigDecimal.valueOf(0.1)).setScale(2, RoundingMode.HALF_UP);
            if (newPrice.signum() <= 0) newPrice = BigDecimal.valueOf(0.01);
        }

        currentPrices.put(symbol, newPrice);
        return new PriceTick(symbol, newPrice, Instant.now());
    }

    public Flux<PriceTick> getPriceTicks() {
        return priceTickSink.asFlux(); //
    }
}
