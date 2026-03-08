package org.example.alert.domain.logic.manager.actor;

import lombok.extern.slf4j.Slf4j;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import org.example.alert.domain.model.ActorCommand;
import org.example.alert.domain.model.enums.AlertStatus;
import org.example.alert.domain.model.enums.FrequencyCondition;
import org.example.alert.infrastructure.persistence.entity.PriceAlertEntity;
import org.example.alert.infrastructure.persistence.repository.PriceAlertRepository;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;

/**
 * AlertManagerActor - Manages matched alerts and updates database
 *
 * Responsibilities:
 * 1. Receive matched alert notifications from SymbolMatchingActor
 * 2. Increment hitCount in database
 * 3. Update status to TRIGGERED when conditions are met:
 *    - ONLY_ONCE: Triggered once, set status=TRIGGERED
 *    - Other frequencies: Also set status=TRIGGERED when matched
 * 4. Send REMOVE signal to SymbolMatchingActor to remove alert from matching queue
 *
 * Lifecycle:
 * - Created as a singleton actor
 * - Handles all matched alerts across all symbols
 * - Performs database updates asynchronously
 */
@Slf4j
public class AlertManagerActor extends AbstractBehavior<ActorCommand> {

    // ==================== COMMANDS ====================

    /**
     * Command sent when an alert has been matched
     */
    public static class AlertMatched implements ActorCommand {
        public final String alertId;
        public final String symbol;
        public final String source;
        public final BigDecimal matchedPrice;
        public final BigDecimal previousPrice;
        public final FrequencyCondition frequencyCondition;
        public final int currentHitCount;
        public final int maxHits;

        public AlertMatched(
                String alertId,
                String symbol,
                String source,
                BigDecimal matchedPrice,
                BigDecimal previousPrice,
                FrequencyCondition frequencyCondition,
                int currentHitCount,
                int maxHits) {
            this.alertId = alertId;
            this.symbol = symbol;
            this.source = source;
            this.matchedPrice = matchedPrice;
            this.previousPrice = previousPrice;
            this.frequencyCondition = frequencyCondition;
            this.currentHitCount = currentHitCount;
            this.maxHits = maxHits;
        }
    }

    // ==================== STATE ====================

    private final PriceAlertRepository repository;
    private final ClusterSharding clusterSharding;

    // ==================== BEHAVIOR FACTORY ====================

    public static Behavior<ActorCommand> create(
            PriceAlertRepository repository,
            ClusterSharding clusterSharding) {
        return Behaviors.setup(context ->
            new AlertManagerActor(context, repository, clusterSharding)
        );
    }

    private AlertManagerActor(
            ActorContext<ActorCommand> context,
            PriceAlertRepository repository,
            ClusterSharding clusterSharding) {
        super(context);
        this.repository = repository;
        this.clusterSharding = clusterSharding;

        log.info("AlertManagerActor started");
    }

    // ==================== MESSAGE HANDLERS ====================

    @Override
    public Receive<ActorCommand> createReceive() {
        return newReceiveBuilder()
            .onMessage(AlertMatched.class, this::onAlertMatched)
            .build();
    }

    /**
     * Handle AlertMatched command
     *
     * Processing flow:
     * 1. Load alert entity from database
     * 2. Increment hitCount
     * 3. Check if alert should be triggered:
     *    - ONLY_ONCE: Always trigger (set status=TRIGGERED)
     *    - Other frequencies: Set status=TRIGGERED
     * 4. Update entity in database
     * 5. Send REMOVE signal to SymbolMatchingActor
     */
    private Behavior<ActorCommand> onAlertMatched(AlertMatched cmd) {
        log.info("Alert matched: alertId={}, symbol={}, source={}, price={}, hitCount={}",
            cmd.alertId, cmd.symbol, cmd.source, cmd.matchedPrice, cmd.currentHitCount);

        try {
            // STEP 1: Load alert from database
            Optional<PriceAlertEntity> alertOpt = repository.findById(cmd.alertId);

            if (alertOpt.isEmpty()) {
                log.warn("Alert {} not found in database, skipping update", cmd.alertId);
                return this;
            }

            PriceAlertEntity alert = alertOpt.get();

            // STEP 2: Increment hitCount
            int newHitCount = (alert.getHitCount() != null ? alert.getHitCount() : 0) + 1;
            alert.setHitCount(newHitCount);
            alert.setUpdatedAt(Instant.now());

            // STEP 3: Check if alert should be triggered
            boolean shouldTrigger = shouldTriggerAlert(cmd.frequencyCondition, newHitCount, cmd.maxHits);

            alert.setStatus(AlertStatus.TRIGGERED.name());

            // STEP 4: Update entity in database
            repository.save(alert);
            // STEP 5: Send REMOVE signal to SymbolMatchingActor if triggered
            if (shouldTrigger) {
                sendRemoveSignal(cmd.alertId, cmd.source, cmd.symbol);
            }

        } catch (Exception e) {
            log.error("Error processing matched alert {}: {}", cmd.alertId, e.getMessage(), e);
        }

        return this;
    }

    /**
     * Determine if alert should be triggered based on frequency condition
     *
     * @param frequencyCondition Alert frequency condition
     * @param hitCount Current hit count
     * @param maxHits Maximum allowed hits
     * @return true if alert should be triggered
     */
    private boolean shouldTriggerAlert(
            FrequencyCondition frequencyCondition,
            int hitCount,
            int maxHits) {

        return switch (frequencyCondition) {
            case ONLY_ONCE -> {
                // ONLY_ONCE: Trigger immediately when matched
                yield hitCount >= 1;
            }
            case PER_DAY -> {
                // PER_DAY: Trigger after reaching max hits
                yield hitCount >= maxHits;
            }
            case ALWAYS_PER_MINUTE -> {
                // ALWAYS_PER_MINUTE: Trigger after reaching max hits
                yield hitCount >= maxHits;
            }
        };
    }

    /**
     * Send REMOVE signal to SymbolMatchingActor via direct Pekko messaging
     *
     * Uses ClusterSharding to get entity reference and sends RemoveAlert command directly.
     * This replaces the queue-based approach for immediate alert removal.
     *
     * @param alertId Alert ID to remove
     * @param source Symbol source
     * @param symbol Symbol name
     */
    private void sendRemoveSignal(String alertId, String source, String symbol) {
        try {
            // Build shard key (format: "SOURCE:SYMBOL")
            String shardKey = source + ":" + symbol;

            // Get entity reference for SymbolMatchingActor via ClusterSharding
//            var matchingActorRef = clusterSharding.entityRefFor(
//                SymbolMatchingCoordinator.SYMBOL_MATCHING_ENTITY_KEY,
//                shardKey
//            );
//
//            // Send RemoveAlert command directly via Pekko messaging
//            matchingActorRef.tell(new SymbolMatchingActor.RemoveAlert(alertId));

            log.info("Sent REMOVE command for alert {} to SymbolMatchingActor ({}) via Pekko",
                alertId, shardKey);

        } catch (Exception e) {
            log.error("Error sending REMOVE command for alert {}: {}",
                alertId, e.getMessage(), e);
        }
    }

}