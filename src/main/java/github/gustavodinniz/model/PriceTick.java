package github.gustavodinniz.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PriceTick {

    private String symbol;
    private BigDecimal price;
    private Instant timestamp;
}
