package github.gustavodinniz.service;

import github.gustavodinniz.aop.MetricsAspect;
import github.gustavodinniz.config.AppProperties;
import github.gustavodinniz.model.PriceTick;
import github.gustavodinniz.model.VolatilityAlert;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

@Slf4j
@Service
@RequiredArgsConstructor
public class VolatilityDetectorService {

    private final MetricsAspect metricsAspect;

    private final PriceTickerSimulatorService priceTickerService;
    private final AppProperties appProperties;

    private final Map<String, Deque<PriceTick>> recentTicksBySymbol = new ConcurrentHashMap<>();

    private final Map<String, Instant> lastAlertTimestampBySymbol = new ConcurrentHashMap<>();
    private static final Duration ALERT_COOLDOWN_DURATION = Duration.ofMinutes(5);

    @PostConstruct
    public void init() {
        log.info("Initializing Volatility Detector Service...");
        int timeWindowMinutes = appProperties.getVolatility().getDefaultTimeWindowMinutes();

        int maxDequeSize = 200;

        priceTickerService.getPriceTicks()
                .publishOn(Schedulers.boundedElastic())
                .flatMap(tick -> processTickForVolatility(tick, maxDequeSize))
                .doOnError(error -> log.error("Error in volatility detection stream: ", error))
                .onErrorContinue((throwable, o) -> log.error("Error processing object {} for volatility, continuing stream.", o, throwable))
                .subscribe(
                        alert -> log.warn("VOLATILITY DETECTED: {}", alert),
                        error -> log.error("Volatility Detector stream stopped due to unrecoverable error: ", error)
                );
        log.info("Volatility Detector Service subscribed to price ticks. Window: {} minutes, Max Deque Size (approx): {}", timeWindowMinutes, maxDequeSize);
    }

    public Mono<VolatilityAlert> processTickForVolatility(PriceTick tick, int maxDequeSize) {
        return Mono.fromCallable(() -> {
            String symbol = tick.getSymbol();
            Deque<PriceTick> ticksHistory = recentTicksBySymbol.computeIfAbsent(symbol, k -> new LinkedBlockingDeque<>(maxDequeSize));

            if (ticksHistory.size() >= maxDequeSize) {
                ticksHistory.pollFirst();
            }
            ticksHistory.offerLast(tick);

            Instant windowStartTime = Instant.now().minus(Duration.ofMinutes(appProperties.getVolatility().getDefaultTimeWindowMinutes()));
            while (!ticksHistory.isEmpty() && ticksHistory.peekFirst().getTimestamp().isBefore(windowStartTime)) {
                ticksHistory.pollFirst();
            }


            if (ticksHistory.size() < 2) {
                return Mono.<VolatilityAlert>empty();
            }

            PriceTick oldestTickInWindow = ticksHistory.peekFirst();
            PriceTick latestTickInWindow = ticksHistory.peekLast();

            if (oldestTickInWindow == null || latestTickInWindow == null || oldestTickInWindow.getPrice().compareTo(BigDecimal.ZERO) == 0) {
                return Mono.<VolatilityAlert>empty();
            }


            BigDecimal priceChange = latestTickInWindow.getPrice().subtract(oldestTickInWindow.getPrice());
            BigDecimal percentageChange = priceChange
                    .divide(oldestTickInWindow.getPrice(), 4, RoundingMode.HALF_UP)
                    .multiply(BigDecimal.valueOf(100))
                    .abs();

            log.trace("Symbol: {}, Oldest Price: {}, Latest Price: {}, %Change: {}",
                    symbol, oldestTickInWindow.getPrice(), latestTickInWindow.getPrice(), percentageChange);

            double threshold = appProperties.getVolatility().getDefaultThresholdPercentage();
            if (percentageChange.doubleValue() >= threshold) {

                Instant lastAlertTime = lastAlertTimestampBySymbol.get(symbol);
                if (lastAlertTime != null && Instant.now().isBefore(lastAlertTime.plus(ALERT_COOLDOWN_DURATION))) {
                    log.debug("Volatility alert for {} suppressed due to cooldown.", symbol);
                    return Mono.<VolatilityAlert>empty();
                }

                lastAlertTimestampBySymbol.put(symbol, Instant.now());

                VolatilityAlert alert = VolatilityAlert.builder()
                        .stockSymbol(symbol)
                        .initialPrice(oldestTickInWindow.getPrice())
                        .finalPrice(latestTickInWindow.getPrice())
                        .percentChange(percentageChange)
                        .timeWindowDescription(appProperties.getVolatility().getDefaultTimeWindowMinutes() + " minutes")
                        .detectedAt(Instant.now())
                        .build();

                metricsAspect.incrementVolatilityAlertsGenerated();

                return Mono.just(alert);
            }
            return Mono.<VolatilityAlert>empty();
        }).flatMap(mono -> mono);
    }
}
