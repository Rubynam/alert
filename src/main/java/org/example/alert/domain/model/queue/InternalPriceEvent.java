package org.example.alert.domain.model.queue;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.example.alert.domain.logic.consumer.actor.IngressActorCommand;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;

/**
 * Simplified price event for queue processing
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class InternalPriceEvent implements Serializable, IngressActorCommand {
    private String symbol;
    private String source;
    private BigDecimal price;
    private Instant timestamp;
    private String eventId;
}