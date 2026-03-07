package org.example.alert.domain.model.queue;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * Price event queued from Kafka topic
 * Stored in PriceQueue per symbol
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PriceEventMessage implements Serializable {

    @JsonProperty("symbol")
    private String symbol;

    @JsonProperty("source")
    private String source;

    @JsonProperty("bidPrice")
    private BigDecimal bidPrice;

    @JsonProperty("askPrice")
    private BigDecimal askPrice;

    @JsonProperty("bidQty")
    private BigDecimal bidQty;

    @JsonProperty("askQty")
    private BigDecimal askQty;

    @JsonProperty("timestamp")
    private Long timestamp;

    @JsonProperty("eventId")
    private String eventId;
}