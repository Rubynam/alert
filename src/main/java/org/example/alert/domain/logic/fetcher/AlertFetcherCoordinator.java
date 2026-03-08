package org.example.alert.domain.logic.fetcher;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import org.apache.pekko.cluster.sharding.typed.javadsl.Entity;
import org.apache.pekko.cluster.sharding.typed.javadsl.EntityTypeKey;
import org.example.alert.domain.logic.fetcher.actor.ActorFetcherCommand;
import org.example.alert.domain.logic.fetcher.actor.AlertFetcherActor;
import org.example.alert.domain.model.ActorCommand;
import org.example.alert.infrastructure.persistence.repository.PriceAlertRepository;
import org.example.alert.infrastructure.queue.AlertUserQueue;
import org.springframework.stereotype.Service;

/**
 * AlertFetcherCoordinator - Orchestrates alert fetching actors across the cluster
 *
 * Responsibilities:
 * 1. Initialize cluster sharding for AlertFetcherActor
 * 2. Track active source-symbol combinations (up to 600,000 actors)
 * 3. Provide API to register symbols for fetching
 * 4. Distribute actors across cluster nodes via sharding
 *
 * Architecture:
 * - One actor per source-symbol combination (e.g., "BINANCE:BTCUSDT")
 * - Actors distributed across cluster using consistent hashing
 * - Each actor fetches 100 alerts every 1 second
 * - Support for up to 600,000 concurrent actors (distinct source-symbol pairs)
 */
@Slf4j
@Service
public class AlertFetcherCoordinator {

    public static final EntityTypeKey<ActorFetcherCommand> ALERT_FETCHER_ENTITY_KEY =
        EntityTypeKey.create(ActorFetcherCommand.class, "AlertFetcher");

    private final ActorSystem<?> actorSystem;
    private final ClusterSharding clusterSharding;
    private final PriceAlertRepository repository;
    private final AlertUserQueue alertUserQueue;


    public AlertFetcherCoordinator(
            ActorSystem<?> actorSystem,
            PriceAlertRepository repository,
            AlertUserQueue alertUserQueue) {
        this.actorSystem = actorSystem;
        this.clusterSharding = ClusterSharding.get(actorSystem);
        this.repository = repository;
        this.alertUserQueue = alertUserQueue;
        // Initialize cluster sharding for AlertFetcherActor
        initializeClusterSharding();
    }

    /**
     * Initialize cluster sharding for AlertFetcherActor
     *
     * Configuration:
     * - Entity Type: AlertFetcher
     * - Sharding Strategy: Based on source:symbol key
     * - Max Actors: 600,000 (configurable via number-of-shards)
     * - Passivation: After 10 minutes of inactivity
     */
    private void initializeClusterSharding() {
        clusterSharding.init(
            Entity.of(ALERT_FETCHER_ENTITY_KEY, entityContext -> {
                // Extract symbol and source from entity ID
                // Entity ID format: "BINANCE:BTCUSDT"
                String[] parts = entityContext.getEntityId().split(":");
                if (parts.length != 2) {
                    log.error("Invalid entity ID format for AlertFetcher: {}",
                        entityContext.getEntityId());
                    throw new IllegalArgumentException(
                        "Entity ID must be in format SOURCE:SYMBOL");
                }
                String source = parts[0];
                String symbol = parts[1];

                log.info("Initializing AlertFetcherActor for {}", entityContext.getEntityId());

                return AlertFetcherActor.create(
                    symbol,
                    source,
                    repository,
                    clusterSharding
                );
            })
        );

        log.info("AlertFetcherActor cluster sharding initialized (max actors: 600,000)");
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down AlertFetcherCoordinator (active symbols:)");
    }
}