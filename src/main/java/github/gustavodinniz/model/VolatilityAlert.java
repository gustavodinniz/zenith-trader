package github.gustavodinniz.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.math.BigDecimal;
import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "volatility_alerts")
public class VolatilityAlert {

    @Id
    private String id;

    private String stockSymbol;
    private BigDecimal initialPrice;
    private BigDecimal finalPrice;
    private BigDecimal percentChange;
    private String timeWindowDescription;
    private Instant detectedAt;
}
