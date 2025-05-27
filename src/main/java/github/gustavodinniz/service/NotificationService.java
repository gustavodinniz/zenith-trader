package github.gustavodinniz.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class NotificationService {

    private final Map<String, Sinks.Many<ServerSentEvent<String>>> userSinks = new ConcurrentHashMap<>();

    public Flux<ServerSentEvent<String>> getNotificationsForUser(String userId) {
        return userSinks.computeIfAbsent(userId, id ->
                        Sinks.many().multicast().onBackpressureBuffer(100)
                )
                .asFlux()
                .doOnSubscribe(subscription -> log.info("User {} subscribed to notifications", userId))
                .doFinally(signalType -> log.info("User {} notification stream finalized with signal: {}", userId, signalType));
    }

    public Mono<Void> sendNotification(String userId, String message) {
        return Mono.fromRunnable(() -> {
            Sinks.Many<ServerSentEvent<String>> sink = userSinks.get(userId);
            if (sink != null) {
                log.info("Sending notification to user {}: {}", userId, message);
                ServerSentEvent<String> event = ServerSentEvent.<String>builder()
                        .event("order_update")
                        .data(message)
                        .build();
                Sinks.EmitResult result = sink.tryEmitNext(event);
                if (result.isFailure()) {
                    log.warn("Failed to emit notification to user {}: {}. Error: {}", userId, message, result);
                }
            } else {
                log.warn("No active notification sink for user {} to send message: {}", userId, message);
            }
        }).subscribeOn(Schedulers.boundedElastic()).then();
    }
}
