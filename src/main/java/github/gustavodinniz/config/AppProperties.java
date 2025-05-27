package github.gustavodinniz.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Map;

@Data
@Component
@ConfigurationProperties(prefix = "zenith-trader")
public class AppProperties {

    private Ticker ticker = new Ticker();
    private PriceSimulator priceSimulator = new PriceSimulator();
    private Volatility volatility = new Volatility();

    @Data
    public static class Ticker {
        private String symbolsToMonitor = "PETR4.SA,VALE3.SA,AAPL";
    }

    @Data
    public static class PriceSimulator {
        private boolean enabled = true;
        private long intervalMs = 1000;
        private Map<String, BigDecimal> basePrices = Map.of(
                "PETR4.SA", BigDecimal.valueOf(35.00),
                "VALE3.SA", BigDecimal.valueOf(60.00),
                "AAPL", BigDecimal.valueOf(170.00)
        );
        private double priceVariationPercentage = 0.5;
    }

    @Data
    public static class Volatility {
        private double defaultThresholdPercentage = 5.0;
        private int defaultTimeWindowMinutes = 15;
    }
}
