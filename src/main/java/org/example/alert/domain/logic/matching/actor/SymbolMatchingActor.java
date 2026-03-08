package org.example.alert.domain.logic.matching.actor;

import lombok.extern.slf4j.Slf4j;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.example.alert.domain.logic.manager.actor.AlertManagerActor;
import org.example.alert.domain.logic.matching.SymbolMatchingState;
import org.example.alert.domain.model.ActorCommand;
import org.example.alert.domain.model.enums.AlertStatus;
import org.example.alert.domain.model.queue.AlertConfig;
import org.example.alert.domain.model.queue.InternalPriceEvent;
import org.example.alert.infrastructure.queue.AlertUserQueue;
import org.example.alert.infrastructure.queue.PriceQueue;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * SymbolMatchingActor - Handles matching logic for a single symbol
 * Responsibilities:
 * 1. Poll price-queue and alert-queue every second
 * 2. Maintain in-memory alert cache for this symbol
 * 3. Match incoming prices against active alerts
 * 4. Track previous price for cross detection
 * ------------------------------------------
 * Lifecycle:
 * - Created on-demand via cluster sharding
 * - Scheduled polling every 1 second
 * - Passivated after 5 minutes of inactivity
 */
@Slf4j
public class SymbolMatchingActor extends AbstractBehavior<ActorCommand> {

    public static class Poll implements ActorCommand {
        private static final Poll INSTANCE = new Poll();

        private Poll() {}

        public static Poll instance() {
            return INSTANCE;
        }
    }

    /**
     * Command to add or update an alert in the matching cache
     * Sent directly from AlertFetcherActor via Pekko messaging
     */
    public static class AddOrUpdateAlert implements ActorCommand {
        public final AlertConfig alertConfig;

        public AddOrUpdateAlert(AlertConfig alertConfig) {
            this.alertConfig = alertConfig;
        }
    }

    /**
     * Command to remove an alert from the matching cache
     * Sent directly from AlertManagerActor via Pekko messaging
     */
    public static class RemoveAlert implements ActorCommand {
        public final String alertId;

        public RemoveAlert(String alertId) {
            this.alertId = alertId;
        }
    }

    /**
     * Get statistics for monitoring
     */
    public static class GetStats implements ActorCommand {
        public GetStats() {}
    }
    // ==================== STATE ====================

    private final String symbol;
    private final String source;
    private final SymbolMatchingState state;

    // Dependencies
    private final PriceQueue priceQueue;
    private final AlertUserQueue alertUserQueue;
    private final ActorRef<ActorCommand> alertManagerActor;

    // Configuration
    private static final Duration POLL_INTERVAL = Duration.ofSeconds(1);

    // ==================== BEHAVIOR FACTORY ====================

    public static Behavior<ActorCommand> create(
            String symbol,
            String source,
            PriceQueue priceQueue,
            AlertUserQueue alertUserQueue,
            ActorRef<ActorCommand> alertManagerActor) {
        return Behaviors.setup(context ->
            new SymbolMatchingActor(context, symbol, source, priceQueue, alertUserQueue, alertManagerActor)
        );
    }

    private SymbolMatchingActor(
            ActorContext<ActorCommand> context,
            String symbol,
            String source,
            PriceQueue priceQueue,
            AlertUserQueue alertUserQueue,
            ActorRef<ActorCommand> alertManagerActor) {
        super(context);
        this.symbol = symbol;
        this.source = source;
        this.state = new SymbolMatchingState(symbol, source);
        this.priceQueue = priceQueue;
        this.alertUserQueue = alertUserQueue;
        this.alertManagerActor = alertManagerActor;

        // Schedule first poll
        schedulePoll();

        log.debug("SymbolMatchingActor started for {}", state.getShardKey());
    }

    // ==================== MESSAGE HANDLERS ====================

    @Override
    public Receive<ActorCommand> createReceive() {
        return newReceiveBuilder()
            .onMessage(Poll.class, this::onPoll)
            .onMessage(AddOrUpdateAlert.class, this::onAddOrUpdateAlert)
            .onMessage(RemoveAlert.class, this::onRemoveAlert)
            .onMessage(GetStats.class, this::onGetStats)
            .build();
    }

    /**
     * Handle Poll command - CORE MATCHING LOGIC
     * Execution flow:
     * 1. Process alert queue changes (ADD/UPDATE/REMOVE)
     * 2. Process price queue events
     * 3. Match prices against active alerts
     */
    private Behavior<ActorCommand> onPoll(Poll cmd) {
        state.setLastPollTime(Instant.now());

        try {
            // STEP 1: Process alert configuration changes
            List<AlertConfig> alertConfigs = alertUserQueue.dequeueAll(source, symbol);
            processAlertConfigs(alertConfigs);

            // STEP 2: Process price events
            List<InternalPriceEvent> internalPriceEvents = priceQueue.dequeueAll(source, symbol);
            if (internalPriceEvents.isEmpty()) {
                log.trace("No price events for {} in this poll", state.getShardKey());
                return scheduleNextPoll();
            }

            state.setTotalPriceEvents(state.getTotalPriceEvents() + internalPriceEvents.size());

            log.debug("Polling {}: {} price events, {} active alerts",
                state.getShardKey(), internalPriceEvents.size(), state.getAlertCount());

            // STEP 3: Process each price event
            for (InternalPriceEvent event : internalPriceEvents) {
                processPriceEvent(event);
            }

        } catch (Exception e) {
            log.error("Error during poll for {}: {}", state.getShardKey(), e.getMessage(), e);
        }

        return scheduleNextPoll();
    }

    /**
     * Process alert configuration changes from alert-user-queue
     */
    private void processAlertConfigs(List<AlertConfig> configs) {
        if (configs.isEmpty()) {
            return;
        }

        log.debug("Processing {} alert config changes for {}",
            configs.size(), state.getShardKey());

        for (AlertConfig config : configs) {
            switch (config.getOperation()) {
                case ADD, UPDATE -> {
                    state.addOrUpdateAlert(config);
                    log.info("Added/Updated alert {} for {}: target={}, condition={}",
                        config.getAlertId(), state.getShardKey(),
                        config.getTargetPrice(), config.getCondition());
                }
                case REMOVE -> {
                    state.removeAlert(config.getAlertId());
                    log.info("Removed alert {} from {}",
                        config.getAlertId(), state.getShardKey());
                }
            }
        }
    }

    /**
     * Process a single price event and match against active alerts
     */
    private void processPriceEvent(InternalPriceEvent event) {
        BigDecimal currentPrice = event.getPrice();
        BigDecimal previousPrice = state.getPreviousPrice();

        // Get enabled alerts
        List<AlertConfig> enabledAlerts = state.getActiveAlerts().values().stream().toList();

        if (enabledAlerts.isEmpty()) {
            log.trace("No enabled alerts for {}, skipping matching", state.getShardKey());
            state.setPreviousPrice(currentPrice);
            return;
        }

        // Match alerts against current price
        List<AlertConfig> matchedAlerts = enabledAlerts.stream()
            .filter(alert -> matchesCondition(alert, currentPrice, previousPrice))
            .toList();

        if (!matchedAlerts.isEmpty()) {
            state.setTotalMatches(state.getTotalMatches() + matchedAlerts.size());

            log.info("Matched {} alerts for {}: price={} (previous={})",
                matchedAlerts.size(), state.getShardKey(), currentPrice, previousPrice);

            // Forward matched alerts to AlertManagerActor
            matchedAlerts.forEach(alert -> {
                log.info("Alert matched: {} - condition: {} - target: {} - current: {}",
                    alert.getAlertId(), alert.getCondition(),
                    alert.getTargetPrice(), currentPrice);

                // Send notification to AlertManagerActor
                alertManagerActor.tell(new AlertManagerActor.AlertMatched(
                    alert.getAlertId(),
                    alert.getSymbol(),
                    alert.getSource(),
                    currentPrice,
                    previousPrice,
                    alert.getFrequencyCondition(),
                    alert.getHitCount(),
                    alert.getMaxHits()
                ));
            });
        }

        // Update previous price
        state.setPreviousPrice(currentPrice);
    }

    /**
     * Check if alert condition matches current price
     */
    private boolean matchesCondition(AlertConfig alert,
                                     BigDecimal currentPrice,
                                     BigDecimal previousPrice) {
        BigDecimal targetPrice = alert.getTargetPrice();

        return switch (alert.getCondition()) {
            case ABOVE -> currentPrice.compareTo(targetPrice) >= 0;
            case BELOW -> currentPrice.compareTo(targetPrice) <= 0;
            case CROSS_ABOVE ->
                previousPrice != null &&
                previousPrice.compareTo(targetPrice) < 0 &&
                currentPrice.compareTo(targetPrice) >= 0;
            case CROSS_BELOW ->
                previousPrice != null &&
                previousPrice.compareTo(targetPrice) >= 0 &&
                currentPrice.compareTo(targetPrice) < 0;
        };
    }

    /**
     * Handle AddOrUpdateAlert command - Direct Pekko messaging from AlertFetcherActor
     * Adds or updates alert in in-memory cache immediately
     */
    private Behavior<ActorCommand> onAddOrUpdateAlert(AddOrUpdateAlert cmd) {
        state.addOrUpdateAlert(cmd.alertConfig);
        log.info("Added/Updated alert {} for {} via direct Pekko message: target={}, condition={}",
            cmd.alertConfig.getAlertId(), state.getShardKey(),
            cmd.alertConfig.getTargetPrice(), cmd.alertConfig.getCondition());
        return this;
    }

    /**
     * Handle RemoveAlert command - Direct Pekko messaging from AlertManagerActor
     * Removes alert from in-memory cache immediately
     */
    private Behavior<ActorCommand> onRemoveAlert(RemoveAlert cmd) {
        state.removeAlert(cmd.alertId);
        log.info("Removed alert {} from {} via direct Pekko message",
            cmd.alertId, state.getShardKey());
        return this;
    }

    /**
     * Handle GetStats - return statistics
     */
    private Behavior<ActorCommand> onGetStats(GetStats cmd) {
        log.info("Stats for {}: alerts={}, events={}, matches={}",
            state.getShardKey(), state.getAlertCount(),
            state.getTotalPriceEvents(), state.getTotalMatches());
        return this;
    }

    // ==================== SCHEDULING ====================

    /**
     * Schedule the next poll after 1 second
     */
    private Behavior<ActorCommand> scheduleNextPoll() {
        schedulePoll();
        return this;
    }

    /**
     * Schedule poll command
     */
    private void schedulePoll() {
        getContext().scheduleOnce(
            POLL_INTERVAL,
            getContext().getSelf(),
            Poll.instance()
        );
    }
}