package org.example.alert.infrastructure.persistence.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.Table;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * PriceAlertEntity - Cassandra/Scylla DB entity for user price alerts
 *
 * Table: user_alert
 * Primary Key: alert_id (UUID)
 *
 * This entity stores user-configured price alerts that will be fetched
 * by AlertFetcherActor and pushed to AlertUserQueue for matching.
 */
@Table("user_alert")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PriceAlertEntity {

    @PrimaryKey
    @Column("alert_id")
    @Builder.Default
    private String alertId = UUID.randomUUID().toString();

    @Column("symbol")
    private String symbol;

    @Column("source")
    private String source;

    @Column("target_price")
    private BigDecimal targetPrice;

    @Column("condition")
    private String condition;

    @Column("frequency_condition")
    private String frequencyCondition;

    @Column("status")
    private String status;

    @Column("hit_count")
    private Integer hitCount;

    @Column("max_hits")
    private Integer maxHits;

    @Column("created_at")
    private Instant createdAt;

    @Column("updated_at")
    private Instant updatedAt;
}