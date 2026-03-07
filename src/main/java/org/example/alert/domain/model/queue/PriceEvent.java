package org.example.alert.domain.model.queue;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;

/**
 * Simplified price event for queue processing
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PriceEvent implements Serializable {
    private String symbol;
    private String source;
    private BigDecimal price;
    private Instant timestamp;
    private String eventId;
}