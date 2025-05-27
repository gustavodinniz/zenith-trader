package github.gustavodinniz.model;

import github.gustavodinniz.enumerated.OrderStatus;
import github.gustavodinniz.enumerated.OrderType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.math.BigDecimal;
import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "stop_orders")
@CompoundIndex(name = "user_symbol_status_idx", def = "{'userId': 1, 'stockSymbol': 1, 'status': 1}")
public class StopOrder {

    @Id
    private String id;

    @Indexed
    private String userId;

    @Indexed
    private String stockSymbol;

    private BigDecimal triggerPrice;

    private OrderType type;

    private Integer quantity;

    @Indexed
    private OrderStatus status;

    @CreatedDate
    private Instant createdAt;

    @LastModifiedDate
    private Instant updatedAt;
}
