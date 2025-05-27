package github.gustavodinniz.aop;

import github.gustavodinniz.model.StopOrder;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Slf4j
@Aspect
@Component
public class MetricsAspect {

    private final MeterRegistry meterRegistry;

    private final Counter ordersCreatedCounter;
    private final Counter ordersCancelledCounter;
    private final Counter ordersTriggeredCounter;
    private final Counter volatilityAlertsGeneratedCounter;

    public MetricsAspect(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;


        this.ordersCreatedCounter = Counter.builder("pulse.orders.created")
                .description("Number of stop orders created")
                .register(meterRegistry);

        this.ordersCancelledCounter = Counter.builder("pulse.orders.cancelled")
                .description("Number of stop orders cancelled")
                .register(meterRegistry);

        this.ordersTriggeredCounter = Counter.builder("pulse.orders.triggered")
                .description("Number of stop orders triggered")
                .register(meterRegistry);

        this.volatilityAlertsGeneratedCounter = Counter.builder("pulse.volatility.alerts.generated")
                .description("Number of volatility alerts generated")
                .register(meterRegistry);
    }

    @Pointcut("execution(* github.gustavodinniz.service.OrderManagementService.createOrder(..)) && args(order)")
    public void createOrderExecution(StopOrder order) {}

    @Pointcut("execution(* github.gustavodinniz.service.OrderManagementService.cancelOrder(..))")
    public void cancelOrderExecution() {}

    @Pointcut("execution(* github.gustavodinniz.repository.StopOrderRepository.save(..)) && args(order)")
    public void saveOrderExecution(StopOrder order) {}

    @Pointcut("execution(* github.gustavodinniz.service.VolatilityDetectorService.processTickForVolatility(..))")
    public void volatilityAlertProcessing() {}

    @Around("createOrderExecution(order)")
    public Object measureOrderCreation(ProceedingJoinPoint joinPoint, StopOrder order) throws Throwable {
        Timer.Sample sample = Timer.start(meterRegistry);
        Object result = null;
        try {
            result = joinPoint.proceed();
            if (result instanceof Mono) {
                return ((Mono<?>) result).doOnSuccess(s -> {
                    ordersCreatedCounter.increment();
                    log.debug("Order created metric incremented for type: {}", order.getType());
                    sample.stop(meterRegistry.timer("pulse.orders.creation.time", "type", order.getType().name(), "status", "success"));
                }).doOnError(e ->
                        sample.stop(meterRegistry.timer("pulse.orders.creation.time", "type", order.getType().name(), "status", "failure"))
                );
            } else {
                ordersCreatedCounter.increment();
                sample.stop(meterRegistry.timer("pulse.orders.creation.time", "type", order.getType().name(), "status", "success"));
            }
            return result;
        } catch (Throwable throwable) {
            sample.stop(meterRegistry.timer("pulse.orders.creation.time", "type", (order != null ? order.getType().name() : "unknown"), "status", "failure"));
            throw throwable;
        }
    }


    @Around("cancelOrderExecution()")
    public Object measureOrderCancellation(ProceedingJoinPoint joinPoint) throws Throwable {
        Timer.Sample sample = Timer.start(meterRegistry);
        Object result = null;
        try {
            result = joinPoint.proceed();
            if (result instanceof Mono) {
                return ((Mono<?>) result).doOnSuccess(s -> {
                    if (s != null) {
                        ordersCancelledCounter.increment();
                        sample.stop(meterRegistry.timer("pulse.orders.cancellation.time", "status", "success"));
                    } else {
                        sample.stop(meterRegistry.timer("pulse.orders.cancellation.time", "status", "not_found_or_not_cancellable"));
                    }
                }).doOnError(e ->
                        sample.stop(meterRegistry.timer("pulse.orders.cancellation.time", "status", "failure"))
                );
            } else {
                ordersCancelledCounter.increment(); // Se síncrono e bem-sucedido
                sample.stop(meterRegistry.timer("pulse.orders.cancellation.time", "status", "success"));
            }
            return result;
        } catch (Throwable throwable) {
            sample.stop(meterRegistry.timer("pulse.orders.cancellation.time", "status", "failure"));
            throw throwable;
        }
    }


    public void incrementOrdersTriggered() {
        ordersTriggeredCounter.increment();
        log.debug("Order triggered metric incremented directly.");
    }

    public void incrementVolatilityAlertsGenerated() {
        volatilityAlertsGeneratedCounter.increment();
        log.debug("Volatility alert metric incremented directly.");
    }

    public Timer.Sample startVolatilityProcessingTimer() {
        return Timer.start(meterRegistry);
    }

    public void stopVolatilityProcessingTimer(Timer.Sample sample, boolean success) {
        sample.stop(meterRegistry.timer("pulse.volatility.processing.time", "status", success ? "success" : "failure"));
    }

}
