package github.gustavodinniz.dto.request;

import github.gustavodinniz.enumerated.OrderType;

public record CreateStopOrderRequest(String userId,
                                     String stockSymbol,
                                     java.math.BigDecimal triggerPrice,
                                     OrderType type,
                                     int quantity) {
}
