package github.gustavodinniz.repository;

import github.gustavodinniz.enumerated.OrderStatus;
import github.gustavodinniz.enumerated.OrderType;
import github.gustavodinniz.model.StopOrder;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.List;

@Repository
public interface StopOrderRepository extends ReactiveMongoRepository<StopOrder, String> {

    Flux<StopOrder> findByUserIdAndStatus(String userId, OrderStatus status);

    Flux<StopOrder> findByStockSymbolAndStatus(String stockSymbol, OrderStatus status);

    @Query("{ 'stockSymbol': ?0, 'status': 'ACTIVE', 'type': 'STOP_LOSS', 'triggerPrice': { $gte: ?1 } }")
    Flux<StopOrder> findActiveStopLossOrdersToTrigger(String stockSymbol, BigDecimal currentPrice);

    @Query("{ 'stockSymbol': ?0, 'status': 'ACTIVE', 'type': 'STOP_GAIN', 'triggerPrice': { $lte: ?1 } }")
    Flux<StopOrder> findActiveStopGainOrdersToTrigger(String stockSymbol, BigDecimal currentPrice);

    Flux<StopOrder> findByStockSymbolAndStatusAndTypeIn(String stockSymbol, OrderStatus status, List<OrderType> types);

    Mono<StopOrder> findByIdAndUserId(String id, String userId);
}
