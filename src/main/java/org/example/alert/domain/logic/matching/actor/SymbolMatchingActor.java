package org.example.alert.domain.logic.matching.actor;

import lombok.extern.slf4j.Slf4j;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.example.alert.domain.logic.matching.SymbolMatchingState;
import org.example.alert.domain.model.enums.AlertStatus;
import org.example.alert.domain.model.queue.AlertConfig;
import org.example.alert.domain.model.queue.PriceEvent;
import org.example.alert.infrastructure.queue.AlertUserQueue;
import org.example.alert.infrastructure.queue.PriceQueue;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * SymbolMatchingActor - Handles matching logic for a single symbol
 *
 * Responsibilities:
 * 1. Poll price-queue and alert-queue every second
 * 2. Maintain in-memory alert cache for this symbol
 * 3. Match incoming prices against active alerts
 * 4. Track previous price for cross detection
 *
 * Lifecycle:
 * - Created on-demand via cluster sharding
 * - Scheduled polling every 1 second
 * - Passivated after 5 minutes of inactivity
 */
@Slf4j
public class SymbolMatchingActor extends AbstractBehavior<SymbolMatchingActor.Command> {

    // ==================== COMMANDS ====================

    public interface Command {}

    /**
     * Triggered every 1 second by coordinator
     * Polls both queues and performs matching
     */
    public static class Poll implements Command {
        private static final Poll INSTANCE = new Poll();

        private Poll() {}

        public static Poll instance() {
            return INSTANCE;
        }
    }

    /**
     * Get statistics for monitoring
     */
    public static class GetStats implements Command {
        public GetStats() {}
    }

    public static class StatsResponse {
        public final String shardKey;
        public final int activeAlertCount;
        public final long totalPriceEvents;
        public final long totalMatches;
        public final Instant lastPollTime;

        public StatsResponse(String shardKey, int activeAlertCount,
                           long totalPriceEvents, long totalMatches,
                           Instant lastPollTime) {
            this.shardKey = shardKey;
            this.activeAlertCount = activeAlertCount;
            this.totalPriceEvents = totalPriceEvents;
            this.totalMatches = totalMatches;
            this.lastPollTime = lastPollTime;
        }
    }

    // ==================== STATE ====================

    private final String symbol;
    private final String source;
    private final SymbolMatchingState state;

    // Dependencies
    private final PriceQueue priceQueue;
    private final AlertUserQueue alertUserQueue;

    // Configuration
    private static final Duration POLL_INTERVAL = Duration.ofSeconds(1);

    // ==================== BEHAVIOR FACTORY ====================

    public static Behavior<Command> create(
            String symbol,
            String source,
            PriceQueue priceQueue,
            AlertUserQueue alertUserQueue) {
        return Behaviors.setup(context ->
            new SymbolMatchingActor(context, symbol, source, priceQueue, alertUserQueue)
        );
    }

    private SymbolMatchingActor(
            ActorContext<Command> context,
            String symbol,
            String source,
            PriceQueue priceQueue,
            AlertUserQueue alertUserQueue) {
        super(context);
        this.symbol = symbol;
        this.source = source;
        this.state = new SymbolMatchingState(symbol, source);
        this.priceQueue = priceQueue;
        this.alertUserQueue = alertUserQueue;

        // Schedule first poll
        schedulePoll();

        log.info("SymbolMatchingActor started for {}", state.getShardKey());
    }

    // ==================== MESSAGE HANDLERS ====================

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
            .onMessage(Poll.class, this::onPoll)
            .onMessage(GetStats.class, this::onGetStats)
            .build();
    }

    /**
     * Handle Poll command - CORE MATCHING LOGIC
     *
     * Execution flow:
     * 1. Process alert queue changes (ADD/UPDATE/REMOVE)
     * 2. Process price queue events
     * 3. Match prices against active alerts
     */
    private Behavior<Command> onPoll(Poll cmd) {
        state.setLastPollTime(Instant.now());

        try {
            // STEP 1: Process alert configuration changes
            List<AlertConfig> alertConfigs = alertUserQueue.dequeueAll(source, symbol);
            processAlertConfigs(alertConfigs);

            // STEP 2: Process price events
            List<PriceEvent> priceEvents = priceQueue.dequeueAll(source, symbol);
            if (priceEvents.isEmpty()) {
                log.trace("No price events for {} in this poll", state.getShardKey());
                return scheduleNextPoll();
            }

            state.setTotalPriceEvents(state.getTotalPriceEvents() + priceEvents.size());

            log.debug("Polling {}: {} price events, {} active alerts",
                state.getShardKey(), priceEvents.size(), state.getAlertCount());

            // STEP 3: Process each price event
            for (PriceEvent event : priceEvents) {
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
    private void processPriceEvent(PriceEvent event) {
        BigDecimal currentPrice = event.getPrice();
        BigDecimal previousPrice = state.getPreviousPrice();

        // Get enabled alerts
        List<AlertConfig> enabledAlerts = state.getActiveAlerts().values().stream()
            .filter(alert -> alert.getStatus() == AlertStatus.ENABLED)
            .toList();

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

            // TODO: Forward matched alerts to AlertManagerActor
            // For now, just log the matches
            matchedAlerts.forEach(alert ->
                log.info("Alert matched: {} - condition: {} - target: {} - current: {}",
                    alert.getAlertId(), alert.getCondition(),
                    alert.getTargetPrice(), currentPrice)
            );
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
     * Handle GetStats - return statistics
     */
    private Behavior<Command> onGetStats(GetStats cmd) {
        log.info("Stats for {}: alerts={}, events={}, matches={}",
            state.getShardKey(), state.getAlertCount(),
            state.getTotalPriceEvents(), state.getTotalMatches());
        return this;
    }

    // ==================== SCHEDULING ====================

    /**
     * Schedule the next poll after 1 second
     */
    private Behavior<Command> scheduleNextPoll() {
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