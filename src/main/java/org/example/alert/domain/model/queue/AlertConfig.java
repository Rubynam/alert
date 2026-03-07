package org.example.alert.domain.model.queue;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.example.alert.domain.model.enums.AlertCondition;
import org.example.alert.domain.model.enums.AlertStatus;
import org.example.alert.domain.model.enums.FrequencyCondition;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;

/**
 * Alert configuration queued from REST API
 * Stored in AlertUserQueue per symbol
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class AlertConfig implements Serializable {

    public enum Operation {
        ADD,      // Add new alert
        UPDATE,   // Update existing alert
        REMOVE    // Remove alert
    }

    private String alertId;
    private String symbol;
    private String source;
    private BigDecimal targetPrice;
    private AlertCondition condition;           // ABOVE, BELOW, CROSS_ABOVE, CROSS_BELOW
    private FrequencyCondition frequencyCondition; // ONLY_ONCE, PER_DAY, ALWAYS_PER_MINUTE
    private AlertStatus status;                 // ENABLED, DISABLED
    private int maxHits;
    private Operation operation;                // ADD, UPDATE, REMOVE
    private Instant queuedAt;                  // When queued
}