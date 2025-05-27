package github.gustavodinniz.service;

import github.gustavodinniz.aop.MetricsAspect;
import github.gustavodinniz.enumerated.OrderStatus;
import github.gustavodinniz.enumerated.OrderType;
import github.gustavodinniz.model.PriceTick;
import github.gustavodinniz.model.StopOrder;
import github.gustavodinniz.repository.StopOrderRepository;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveZSetOperations;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Slf4j
@Service
@RequiredArgsConstructor
public class StopOrderProcessorService {

    private final PriceTickerSimulatorService priceTickerService;
    private final StopOrderRepository stopOrderRepository;
    private final NotificationService notificationService;

    private final MetricsAspect metricsAspect;

    private final ReactiveRedisTemplate<String, Object> reactiveRedisTemplate;

    private String getRedisKeyForActiveOrders(String symbol, OrderType type) {
        return "active_orders:" + type.name() + ":" + symbol;
    }

    @PostConstruct
    public void init() {
        log.info("Initializing Stop Order Processor...");
        priceTickerService.getPriceTicks()
                .publishOn(Schedulers.parallel())
                .flatMap(this::processPriceTick, 5)
                .doOnError(error -> log.error("Error in stop order processing stream: ", error))
                .onErrorContinue((throwable, o) -> log.error("Error processing object {}, continuing stream.", o, throwable))
                .subscribe(
                        triggeredOrder -> log.info("Order {} processed for trigger.", triggeredOrder.getId()),
                        error -> log.error("Stop Order Processor stream stopped due to unrecoverable error: ", error)
                );
        log.info("Stop Order Processor subscribed to price ticks.");
    }

    public Mono<Void> addOrderToRedisCache(StopOrder order) {
        if (order.getStatus() != OrderStatus.ACTIVE) {
            return Mono.empty();
        }
        String redisKey = getRedisKeyForActiveOrders(order.getStockSymbol(), order.getType());
        return reactiveRedisTemplate.opsForZSet()
                .add(redisKey, order.getId(), order.getTriggerPrice().doubleValue())
                .doOnSuccess(added -> {
                    if (Boolean.TRUE.equals(added))
                        log.debug("Order {} added to Redis cache [{}].", order.getId(), redisKey);
                    else log.warn("Order {} NOT added/updated in Redis cache [{}].", order.getId(), redisKey);
                })
                .then();
    }

    public Mono<Void> removeOrderFromRedisCache(StopOrder order) {
        String redisKey = getRedisKeyForActiveOrders(order.getStockSymbol(), order.getType());
        return reactiveRedisTemplate.opsForZSet()
                .remove(redisKey, order.getId())
                .doOnSuccess(removedCount -> log.debug("Removed {} instance(s) of order {} from Redis cache [{}].", removedCount, order.getId(), redisKey))
                .then();
    }


    private Flux<StopOrder> processPriceTick(PriceTick tick) {
        log.trace("Processing tick: {}", tick);

        Flux<StopOrder> stopLossOrders = findAndProcessTriggeredOrders(tick, OrderType.STOP_LOSS);
        Flux<StopOrder> stopGainOrders = findAndProcessTriggeredOrders(tick, OrderType.STOP_GAIN);

        return Flux.merge(stopLossOrders, stopGainOrders);
    }

    private Flux<StopOrder> findAndProcessTriggeredOrders(PriceTick tick, OrderType type) {
        String redisKey = getRedisKeyForActiveOrders(tick.getSymbol(), type);
        ReactiveZSetOperations<String, Object> zSetOps = reactiveRedisTemplate.opsForZSet();

        Flux<String> orderIdsToTrigger;

        Range<Double> scoreRange;
        Range.Bound<Double> lowerBound;
        Range.Bound<Double> upperBound;

        if (type == OrderType.STOP_LOSS) {
            log.trace("Checking STOP_LOSS for {}. Symbol: {}, Tick Price: {}, Redis Key: {}", type, tick.getSymbol(), tick.getPrice(), redisKey);
            lowerBound = Range.Bound.inclusive(tick.getPrice().doubleValue());
            upperBound = Range.Bound.unbounded();
            scoreRange = Range.of(lowerBound, upperBound);
        } else {

            log.trace("Checking STOP_GAIN for {}. Symbol: {}, Tick Price: {}, Redis Key: {}", type, tick.getSymbol(), tick.getPrice(), redisKey);
            lowerBound = Range.Bound.unbounded();
            upperBound = Range.Bound.inclusive(tick.getPrice().doubleValue());
            scoreRange = Range.of(lowerBound, upperBound);
        }

        orderIdsToTrigger = zSetOps.rangeByScore(redisKey, scoreRange)
                .map(String::valueOf);

        return orderIdsToTrigger
                .concatMap(orderId ->
                        stopOrderRepository.findById(orderId)
                                .filter(order -> order.getStatus() == OrderStatus.ACTIVE)
                                .flatMap(order -> {
                                    log.info("Potential trigger for order: {} [Type: {}, Trigger: {}, Symbol: {}, Current Price: {}]",
                                            order.getId(), order.getType(), order.getTriggerPrice(), order.getStockSymbol(), tick.getPrice());
                                    order.setStatus(OrderStatus.TRIGGERED);
                                    return stopOrderRepository.save(order)
                                            .doOnSuccess(savedOrder -> {
                                                log.info("Order {} status updated to TRIGGERED.", savedOrder.getId());
                                                metricsAspect.incrementOrdersTriggered();
                                                notificationService.sendNotification(
                                                        savedOrder.getUserId(),
                                                        String.format("Order TRIGGERED: %s %d %s @ %.2f (Market: %.2f)",
                                                                savedOrder.getType(), savedOrder.getQuantity(), savedOrder.getStockSymbol(),
                                                                savedOrder.getTriggerPrice(), tick.getPrice())
                                                ).subscribe();

                                                removeOrderFromRedisCache(savedOrder).subscribe(
                                                        null,
                                                        err -> log.error("Error removing order {} from Redis cache after trigger", savedOrder.getId(), err)
                                                );

                                            })
                                            .doOnError(OptimisticLockingFailureException.class, e ->
                                                    log.warn("Optimistic lock failure triggering order {}: {}", order.getId(), e.getMessage())
                                            )
                                            .onErrorResume(e -> {
                                                log.error("Failed to save triggered order {}: {}", order.getId(), e.getMessage());

                                                return Mono.empty();
                                            });
                                })
                )
                .doOnError(err -> log.error("Error finding/processing orders for tick {}: ", tick, err));
    }
}
