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

    @Query("SELECT count(1) FROM user_alert WHERE status = ?0 ALLOW FILTERING")
    long countByStatus(String status);

    /**
     * Find alerts by status with pagination
     * Used by ClusterBatchFetcherActor for distributed batch fetching
     *
     * @param status Alert status (e.g., "ENABLED")
     * @param pageable Pagination settings for workload distribution
     * @return Slice of alerts with the specified status
     */
    @Query("SELECT * FROM user_alert WHERE status = ?0 ALLOW FILTERING")
    Slice<PriceAlertEntity> findByStatus(String status, Pageable pageable);
}