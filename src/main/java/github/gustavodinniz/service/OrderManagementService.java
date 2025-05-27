package github.gustavodinniz.service;

import github.gustavodinniz.enumerated.OrderStatus;
import github.gustavodinniz.exception.OrderCannotBeCancelledException;
import github.gustavodinniz.exception.OrderNotFoundException;
import github.gustavodinniz.model.StopOrder;
import github.gustavodinniz.repository.StopOrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderManagementService {

    private final StopOrderRepository stopOrderRepository;

    public Mono<StopOrder> createOrder(StopOrder order) {
        log.info("Creating new stop order: {}", order);
        if (order.getTriggerPrice() == null || order.getTriggerPrice().signum() <= 0) {
            log.error("Invalid trigger price for order: {}", order);
            return Mono.error(new IllegalArgumentException("Trigger price must be positive."));
        }

        if (order.getQuantity() <= 0) {
            return Mono.error(new IllegalArgumentException("Quantity must be positive."));
        }

        order.setStatus(OrderStatus.ACTIVE);

        return stopOrderRepository.save(order)
                .doOnSuccess(savedOrder -> log.info("Stop order created successfully: {}", savedOrder.getId()))
                .doOnError(error -> log.error("Error creating stop order: {}", order, error));

    }

    public Mono<StopOrder> cancelOrder(String orderId, String userId) {
        log.debug("Attempting to cancel orderId: {} for userId: {}", orderId, userId);
        return stopOrderRepository.findByIdAndUserId(orderId, userId)
                .switchIfEmpty(Mono.error(new OrderNotFoundException("Order not found or user mismatch for ID: " + orderId)))
                .flatMap(order -> {
                    if (order.getStatus() == OrderStatus.ACTIVE) {
                        order.setStatus(OrderStatus.CANCELLED);

                        return stopOrderRepository.save(order)
                                .doOnSuccess(cancelledOrder -> {
                                    log.info("Order {} cancelled successfully for user {}", orderId, userId);
                                })
                                .doOnError(OptimisticLockingFailureException.class, e ->
                                        log.warn("Optimistic lock failure cancelling order {}: {}", orderId, e.getMessage())
                                )
                                .doOnError(error -> log.error("Error cancelling order {}:", orderId, error));
                    } else if (order.getStatus() == OrderStatus.CANCELLED) {
                        log.warn("Order {} is already cancelled for user {}", orderId, userId);
                        return Mono.just(order);
                    } else {
                        log.warn("Cannot cancel order {} for user {} as its status is {}", orderId, userId, order.getStatus());
                        return Mono.error(new OrderCannotBeCancelledException("Order cannot be cancelled, status: " + order.getStatus()));
                    }
                });
    }

    public Mono<StopOrder> getOrderByIdAndUser(String orderId, String userId) {
        log.debug("Fetching order by ID: {} for user: {}", orderId, userId);
        return stopOrderRepository.findByIdAndUserId(orderId, userId)
                .switchIfEmpty(Mono.error(new OrderNotFoundException("Order not found or user mismatch for ID: " + orderId)));
    }

    public Flux<StopOrder> getOrdersByUserId(String userId, OrderStatus status) {
        log.debug("Fetching orders for user: {} with status: {}", userId, status);
        if (status != null) {
            return stopOrderRepository.findByUserIdAndStatus(userId, status);
        }

        return stopOrderRepository.findByUserIdAndStatus(userId, OrderStatus.ACTIVE);
    }
}
