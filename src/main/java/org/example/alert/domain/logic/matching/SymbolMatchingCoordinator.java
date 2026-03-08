package org.example.alert.domain.logic.matching;

import lombok.extern.slf4j.Slf4j;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.DispatcherSelector;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import org.apache.pekko.cluster.sharding.typed.javadsl.Entity;
import org.apache.pekko.cluster.sharding.typed.javadsl.EntityTypeKey;
import org.example.alert.domain.logic.manager.actor.AlertManagerActor;
import org.example.alert.domain.logic.matching.actor.SymbolMatchingActor;
import org.example.alert.domain.model.ActorCommand;
import org.example.alert.infrastructure.persistence.repository.PriceAlertRepository;
import org.example.alert.infrastructure.queue.AlertUserQueue;
import org.example.alert.infrastructure.queue.PriceQueue;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SymbolMatchingCoordinator - Orchestrates matching actors
 *
 * Responsibilities:
 * 1. Initialize cluster sharding for SymbolMatchingActor
 * 2. Track active symbols (auto-discovery from queues)
 * 3. Provide API to query statistics
 */
@Slf4j
@Service
public class SymbolMatchingCoordinator {

    public static final EntityTypeKey<ActorCommand> SYMBOL_MATCHING_ENTITY_KEY =
        EntityTypeKey.create(ActorCommand.class, "SymbolMatching");

    private final ActorSystem<?> actorSystem;
    private final ClusterSharding clusterSharding;
    private final PriceQueue priceQueue;
    private final AlertUserQueue alertUserQueue;
    private final ActorRef<ActorCommand> alertManagerActor;

    // Track active symbols
    private final Set<String> activeSymbols;

    public SymbolMatchingCoordinator(
            ActorSystem<?> actorSystem,
            PriceQueue priceQueue,
            AlertUserQueue alertUserQueue,
            PriceAlertRepository priceAlertRepository) {
        this.actorSystem = actorSystem;
        this.clusterSharding = ClusterSharding.get(actorSystem);
        this.priceQueue = priceQueue;
        this.alertUserQueue = alertUserQueue;
        this.activeSymbols = ConcurrentHashMap.newKeySet();

        // Create AlertManagerActor as singleton
        this.alertManagerActor = actorSystem.systemActorOf(
            AlertManagerActor.create(priceAlertRepository, clusterSharding),
            "alertManager",
            DispatcherSelector.defaultDispatcher()
        );

        log.info("AlertManagerActor created");

        // Initialize cluster sharding
        initializeClusterSharding();
    }

    /**
     * Initialize cluster sharding for SymbolMatchingActor
     */
    private void initializeClusterSharding() {
        clusterSharding.init(
            Entity.of(SYMBOL_MATCHING_ENTITY_KEY, entityContext -> {
                // Extract symbol and source from entity ID
                // Entity ID format: "BINANCE:BTCUSDT"
                String[] parts = entityContext.getEntityId().split(":");
                if (parts.length != 2) {
                    log.error("Invalid entity ID format: {}", entityContext.getEntityId());
                    throw new IllegalArgumentException("Entity ID must be in format SOURCE:SYMBOL");
                }
                String source = parts[0];
                String symbol = parts[1];

                log.debug("Initializing SymbolMatchingActor for {}", entityContext.getEntityId());

                return SymbolMatchingActor.create(
                    symbol,
                    source,
                    priceQueue,
                    alertUserQueue,
                    alertManagerActor
                );
            })
        );

        log.info("SymbolMatchingActor cluster sharding initialized");
    }

    /**
     * Register a symbol for matching
     * Called when first event arrives for a symbol
     */
    public void registerSymbol(String source, String symbol) {
        String shardKey = source + ":" + symbol;

        if (activeSymbols.add(shardKey)) {
            log.debug("Registered new symbol for matching: {}", shardKey);

            // Trigger actor creation by sending initial poll
            var matchingRef = clusterSharding.entityRefFor(
                SYMBOL_MATCHING_ENTITY_KEY,
                shardKey
            );
            matchingRef.tell(SymbolMatchingActor.Poll.instance());
        }
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down SymbolMatchingCoordinator");
    }
}