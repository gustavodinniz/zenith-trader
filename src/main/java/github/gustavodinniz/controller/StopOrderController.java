package github.gustavodinniz.controller;

import github.gustavodinniz.dto.request.CreateStopOrderRequest;
import github.gustavodinniz.enumerated.OrderStatus;
import github.gustavodinniz.exception.OrderCannotBeCancelledException;
import github.gustavodinniz.exception.OrderNotFoundException;
import github.gustavodinniz.model.StopOrder;
import github.gustavodinniz.service.NotificationService;
import github.gustavodinniz.service.OrderManagementService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;


@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/orders")
public class StopOrderController {

    private final OrderManagementService orderManagementService;
    private final NotificationService notificationService;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<ResponseEntity<StopOrder>> createStopOrder(@RequestBody CreateStopOrderRequest request) {

        StopOrder order = StopOrder.builder()
                .userId(request.userId())
                .stockSymbol(request.stockSymbol())
                .triggerPrice(request.triggerPrice())
                .type(request.type())
                .quantity(request.quantity())
                .build();

        return orderManagementService.createOrder(order)
                .map(savedOrder -> ResponseEntity.status(HttpStatus.CREATED).body(savedOrder))
                .onErrorResume(IllegalArgumentException.class, e ->
                        Mono.just(ResponseEntity.badRequest().<StopOrder>build()))
                .doOnError(error -> log.error("API Error creating order: {}", error.getMessage()));
    }

    @GetMapping("/{orderId}/users/{userId}")
    public Mono<ResponseEntity<StopOrder>> getOrderById(@PathVariable String orderId, @PathVariable String userId) {
        return orderManagementService.getOrderByIdAndUser(orderId, userId)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @GetMapping("/users/{userId}")
    public Flux<StopOrder> getOrdersByUser(
            @PathVariable String userId,
            @RequestParam(required = false) OrderStatus status) {
        return orderManagementService.getOrdersByUserId(userId, status);
    }

    @PatchMapping("/{orderId}/users/{userId}/cancel")
    public Mono<ResponseEntity<StopOrder>> cancelOrder(@PathVariable String orderId, @PathVariable String userId) {
        return orderManagementService.cancelOrder(orderId, userId)
                .map(ResponseEntity::ok)
                .onErrorResume(OrderNotFoundException.class, e ->
                        Mono.just(ResponseEntity.notFound().build()))
                .onErrorResume(OrderCannotBeCancelledException.class, e ->
                        Mono.just(ResponseEntity.status(HttpStatus.CONFLICT).build()));
    }

    @GetMapping(value = "/users/{userId}/notifications", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<String>> streamNotifications(@PathVariable String userId) {
        log.info("SSE connection established for user: {}", userId);

        Flux<ServerSentEvent<String>> heartbeat = Flux.interval(Duration.ofSeconds(15))
                .map(i -> ServerSentEvent.<String>builder()
                        .event("heartbeat")
                        .data("ping")
                        .build())
                .doOnSubscribe(s -> log.debug("Heartbeat SSE subscribed for user {}", userId))
                .doFinally(signalType -> log.debug("Heartbeat SSE finalized for user {}: {}", userId, signalType));


        return Flux.merge(notificationService.getNotificationsForUser(userId), heartbeat)
                .doOnCancel(() -> log.info("SSE connection cancelled for user: {}", userId))
                .doOnError(e -> log.error("Error in SSE stream for user {}: {}", userId, e.getMessage()))
                .doFinally(signalType -> log.info("SSE stream finalized for user {}: {}", userId, signalType));
    }
}

