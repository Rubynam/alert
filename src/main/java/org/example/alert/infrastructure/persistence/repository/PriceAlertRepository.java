package org.example.alert.infrastructure.persistence.repository;

import org.example.alert.infrastructure.persistence.entity.PriceAlertEntity;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * PriceAlertRepository - Spring Data Cassandra repository for PriceAlertEntity
 *
 * Provides methods for querying user_alert table with support for:
 * - Fetching alerts by source and symbol (for AlertFetcherActor)
 * - FIFO ordering (DESC by updated_at)
 * - Batch fetching with pagination (100 per batch)
 */
@Repository
public interface PriceAlertRepository extends CassandraRepository<PriceAlertEntity, String> {

    /**
     * Find alerts by source and symbol, ordered by updated_at DESC (FIFO)
     * Used by AlertFetcherActor for batch fetching
     *
     * @param source Symbol source (e.g., "BINANCE")
     * @param symbol Symbol name (e.g., "BTCUSDT")
     * @param pageable Pagination settings (batch size = 100)
     * @return Slice of alerts for this source-symbol combination
     */
    @Query("SELECT * FROM user_alert WHERE source = ?0 AND symbol = ?1 ORDER BY updated_at DESC")
    Slice<PriceAlertEntity> findBySourceAndSymbolOrderByUpdatedAtDesc(
        String source,
        String symbol,
        Pageable pageable
    );
}