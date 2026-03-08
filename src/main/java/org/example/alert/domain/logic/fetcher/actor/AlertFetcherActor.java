package org.example.alert.domain.logic.fetcher.actor;

import lombok.extern.slf4j.Slf4j;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import org.example.alert.domain.logic.matching.SymbolMatchingCoordinator;
import org.example.alert.domain.logic.matching.actor.SymbolMatchingActor;
import org.example.alert.domain.model.ActorCommand;
import org.example.alert.domain.model.enums.AlertCondition;
import org.example.alert.domain.model.enums.AlertStatus;
import org.example.alert.domain.model.enums.FrequencyCondition;
import org.example.alert.domain.model.queue.AlertConfig;
import org.example.alert.infrastructure.persistence.entity.PriceAlertEntity;
import org.example.alert.infrastructure.persistence.repository.PriceAlertRepository;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Slice;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * AlertFetcherActor - Fetches user alerts from Scylla DB for a specific source-symbol

 * Responsibilities:
 * 1. Fetch alerts from database every 1 second
 * 2. Query by source and symbol (one actor per source-symbol combination)
 * 3. FIFO ordering (DESC by updated_at)
 * 4. Batch size of 100 alerts per fetch
 * 5. Push fetched alerts to AlertUserQueue

 * Clustering:
 * - One actor per source-symbol combination
 * - Distributed across cluster via sharding
 * - Supports up to 600,000 actors (max distinct source-symbol combinations)

 * Lifecycle:
 * - Created on-demand via cluster sharding
 * - Scheduled fetching every 1 second
 * - Passivated after idle timeout
 */
@Slf4j
public class AlertFetcherActor extends AbstractBehavior<ActorCommand> {

    // ==================== COMMANDS ====================

    /**
     * Command to fetch alerts from database
     */
    public static class FetchAlerts implements ActorCommand {
        private static final FetchAlerts INSTANCE = new FetchAlerts();

        private FetchAlerts() {}

        public static FetchAlerts instance() {
            return INSTANCE;
        }
    }

    /**
     * Get statistics for monitoring
     */
    public static class GetStats implements ActorCommand {
        public GetStats() {}
    }

    // ==================== STATE ====================

    private final String source;
    private final String symbol;
    private final String shardKey;  // "BINANCE:BTCUSDT"

    // Dependencies
    private final PriceAlertRepository repository;
    private final ClusterSharding clusterSharding;

    // Configuration
    private static final Duration FETCH_INTERVAL = Duration.ofSeconds(1);
    private static final int BATCH_SIZE = 100;

    private Instant lastFetchTime;

    // ==================== BEHAVIOR FACTORY ====================

    public static Behavior<ActorCommand> create(
            String symbol,
            String source,
            PriceAlertRepository repository,
            ClusterSharding clusterSharding) {
        return Behaviors.setup(context ->
            new AlertFetcherActor(context, symbol, source, repository, clusterSharding)
        );
    }

    private AlertFetcherActor(
            ActorContext<ActorCommand> context,
            String symbol,
            String source,
            PriceAlertRepository repository,
            ClusterSharding clusterSharding) {
        super(context);
        this.symbol = symbol;
        this.source = source;
        this.shardKey = source + ":" + symbol;
        this.repository = repository;
        this.clusterSharding = clusterSharding;
        this.lastFetchTime = Instant.now();

        // Schedule first fetch
        scheduleFetch();

        log.info("AlertFetcherActor started for {}", shardKey);
    }

    // ==================== MESSAGE HANDLERS ====================

    @Override
    public Receive<ActorCommand> createReceive() {
        return newReceiveBuilder()
            .onMessage(FetchAlerts.class, this::onFetchAlerts)
            .onMessage(GetStats.class, this::onGetStats)
            .build();
    }

    /**
     * Handle FetchAlerts command - CORE FETCHING LOGIC

     * Execution flow:
     * 1. Fetch alerts from Scylla DB (batch size 100, FIFO order)
     * 2. Convert PriceAlertEntity to AlertConfig
     * 3. Push each alert to AlertUserQueue with ADD operation
     * 4. Schedule next fetch after 1 second
     */
    private Behavior<ActorCommand> onFetchAlerts(FetchAlerts cmd) {
        lastFetchTime = Instant.now();
        try {
            // STEP 1: Fetch alerts from database (FIFO: DESC by updated_at)
            List<PriceAlertEntity> alerts = fetchAlertsFromDatabase();

            if (alerts.isEmpty()) {
                log.trace("No alerts found for {} in database", shardKey);
                return scheduleNextFetch();
            }

            log.debug("Fetched {} alerts for {}",
                alerts.size(), shardKey);

            pushAlertsToQueue(alerts);

        } catch (Exception e) {
            log.error("Error fetching alerts for {}: {}", shardKey, e.getMessage(), e);
        }

        return scheduleNextFetch();
    }

    /**
     * Fetch alerts from Scylla DB with FIFO ordering

     * Strategy:
     * - Query by source and symbol
     * - Order by updated_at DESC (most recent first)
     * - Batch size: 100 alerts
     * - Uses pagination for efficient memory usage
     */
    private List<PriceAlertEntity> fetchAlertsFromDatabase() {
        try {
            // Create pageable for batch fetching
            PageRequest pageRequest = PageRequest.of(0, BATCH_SIZE);

            // Fetch from database (FIFO: DESC order)
            Slice<PriceAlertEntity> alertSlice = repository
                .findBySourceAndSymbolOrderByUpdatedAtDesc(source, symbol, pageRequest);

            return alertSlice.getContent();

        } catch (Exception e) {
            log.error("Database fetch error for {}: {}", shardKey, e.getMessage());
            throw e;
        }
    }

    /**
     * Convert PriceAlertEntity to AlertConfig and send to SymbolMatchingActor via Pekko
     *
     * @param alerts List of alerts fetched from database
     */
    private void pushAlertsToQueue(List<PriceAlertEntity> alerts) {
        for (PriceAlertEntity entity : alerts) {
            try {
                // Convert entity to AlertConfig
                AlertConfig config = AlertConfig.builder()
                    .alertId(entity.getAlertId())
                    .symbol(entity.getSymbol())
                    .source(entity.getSource())
                    .targetPrice(entity.getTargetPrice())
                    .condition(AlertCondition.valueOf(entity.getCondition()))
                    .frequencyCondition(FrequencyCondition.valueOf(entity.getFrequencyCondition()))
                    .status(AlertStatus.valueOf(entity.getStatus()))
                    .hitCount(entity.getHitCount() != null ? entity.getHitCount() : 0)
                    .maxHits(entity.getMaxHits() != null ? entity.getMaxHits() : 10)
                    .operation(AlertConfig.Operation.ADD)  // Always ADD for fetched alerts
                    .queuedAt(Instant.now())
                    .build();

                // Get entity reference for SymbolMatchingActor via ClusterSharding
                var matchingActorRef = clusterSharding.entityRefFor(
                    SymbolMatchingCoordinator.SYMBOL_MATCHING_ENTITY_KEY,
                    shardKey
                );

                // Send AddOrUpdateAlert command directly via Pekko messaging
                matchingActorRef.tell(new SymbolMatchingActor.AddOrUpdateAlert(config));

                log.trace("Sent AddOrUpdateAlert for alert {} to SymbolMatchingActor ({}) via Pekko",
                    entity.getAlertId(), shardKey);

            } catch (Exception e) {
                log.error("Error sending alert {} to SymbolMatchingActor: {}",
                    entity.getAlertId(), e.getMessage());
            }
        }
    }

    /**
     * Handle GetStats - return statistics
     */
    private Behavior<ActorCommand> onGetStats(GetStats cmd) {
        log.info("Stats for AlertFetcherActor {}: , last_fetch={}",
            shardKey,  lastFetchTime);
        return this;
    }

    // ==================== SCHEDULING ====================

    /**
     * Schedule the next fetch after 1 second
     */
    private Behavior<ActorCommand> scheduleNextFetch() {
        scheduleFetch();
        return this;
    }

    /**
     * Schedule fetch command every 1 second
     */
    private void scheduleFetch() {
        getContext().scheduleOnce(
            FETCH_INTERVAL,
            getContext().getSelf(),
            FetchAlerts.instance()
        );
    }
}