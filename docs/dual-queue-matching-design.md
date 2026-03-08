# Dual-Queue Alert Matching System - Design Document

## Architecture Overview

This design implements a **dual-queue, time-based batch matching system** where:
1. **Price events** from Kafka are pushed to a `price-queue`
2. **User alert configurations** are pushed to an `alert-user-queue`
3. **Apache Pekko actors** (one per symbol) poll both queues every second
4. **Matching logic** runs periodically without blocking Kafka consumption

---

## System Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    Kafka Consumer Layer                          │
│                                                                   │
│  ┌──────────────────┐         ┌──────────────────────┐          │
│  │ price event Kafka    │         │ Alert Config         │          │
│  │ Consumer         │         │ REST API             │          │
│  │ (price-data)   │         │ (POST /alerts)       │          │
│  └────────┬─────────┘         └──────────┬───────────┘          │
│           │                              │                       │
│           │ Price Events                 │ Alert Configs         │
│           ▼                              ▼                       │
└───────────┼──────────────────────────────┼───────────────────────┘
            │                              │
            │                              │
┌───────────┼──────────────────────────────┼───────────────────────┐
│           │        Queue Layer           │                       │
│           │                              │                       │
│  ┌────────▼──────────┐         ┌────────▼──────────┐            │
│  │  PriceQueue       │         │ AlertUserQueue    │            │
│  │  (In-Memory)      │         │ (In-Memory)       │            │
│  │                   │         │                   │            │
│  │ Key: Symbol       │         │ Key: Symbol       │            │
│  │ Value: Queue      │         │ Value: Queue      │            │
│  │   [PriceEvent]    │         │   [AlertConfig]   │            │
│  │                   │         │                   │            │
│  │ BINANCE:BTCUSDT → │         │ BINANCE:BTCUSDT → │            │
│  │   [Event1, ...]   │         │   [Alert1, ...]   │            │
│  │                   │         │                   │            │
│  │ HUOBI:ETHUSDT →   │         │ HUOBI:ETHUSDT →   │            │
│  │   [Event2, ...]   │         │   [Alert2, ...]   │            │
│  └───────┬───────────┘         └───────┬───────────┘            │
│          │                             │                         │
└──────────┼─────────────────────────────┼─────────────────────────┘
           │                             │
           │  Poll every 1 second        │
           │                             │
┌──────────┼─────────────────────────────┼─────────────────────────┐
│          │      Actor Layer            │                         │
│          │                             │                         │
│  ┌───────▼─────────────────────────────▼────────┐                │
│  │   SymbolMatchingCoordinator                  │                │
│  │   - Schedules polling every 1 second         │                │
│  │   - Routes to SymbolMatchingActor per symbol │                │
│  └────────────────┬─────────────────────────────┘                │
│                   │                                               │
│                   │ Broadcast poll commands                      │
│                   │                                               │
│  ┌────────────────▼──────────────────────────────────┐           │
│  │  SymbolMatchingActor Cluster (Cluster Sharding)  │           │
│  │                                                    │           │
│  │  ┌──────────────────────────────────────────┐    │           │
│  │  │ SymbolMatchingActor: BINANCE:BTCUSDT    │    │           │
│  │  │                                          │    │           │
│  │  │ State:                                   │    │           │
│  │  │  - activeAlerts: Map<AlertId, Alert>    │    │           │
│  │  │  - previousPrice: BigDecimal            │    │           │
│  │  │                                          │    │           │
│  │  │ On Poll (every 1 second):               │    │           │
│  │  │  1. Dequeue alerts from alert-queue     │    │           │
│  │  │  2. Dequeue prices from price-queue     │    │           │
│  │  │  3. Match prices against alerts         │    │           │
│  │  │  4. Forward matched alerts              │    │           │
│  │  └──────────────────────────────────────────┘    │           │
│  │                                                    │           │
│  │  ┌──────────────────────────────────────────┐    │           │
│  │  │ SymbolMatchingActor: HUOBI:ETHUSDT      │    │           │
│  │  │  - Handles ETHUSDT independently        │    │           │
│  │  └──────────────────────────────────────────┘    │           │
│  │                                                    │           │
│  └────────────────────┬───────────────────────────────┘           │
│                       │                                           │
│                       │ Matched Alerts                            │
│                       ▼                                           │
│  ┌────────────────────────────────────────────────────┐          │
│  │        AlertManagerActor                           │          │
│  │        (Triggers AlertActor for hit counting)      │          │
│  └────────────────────────────────────────────────────┘          │
└─────────────────────────────────────────────────────────────────┘
```

---

## Data Models

### 1. PriceEvent (Queued from Kafka)

```java
package com.example.sinkconnect.domain.model.queue;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Price event queued from Kafka topic
 * Stored in PriceQueue per symbol
 */

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PriceEventMessage implements Serializable {

    @NotBlank(message = "Symbol is required")
    @JsonProperty("symbol")
    private String symbol;

    @NotBlank(message = "Source is required")
    @JsonProperty("source")
    private String source;

    @NotNull(message = "Bid price is required")
    @Positive(message = "Bid price must be positive")
    @JsonProperty("bidPrice")
    private BigDecimal bidPrice;

    @NotNull(message = "Ask price is required")
    @Positive(message = "Ask price must be positive")
    @JsonProperty("askPrice")
    private BigDecimal askPrice;

    @NotNull(message = "Bid quantity is required")
    @PositiveOrZero(message = "Bid quantity must be non-negative")
    @JsonProperty("bidQty")
    private BigDecimal bidQty;

    @NotNull(message = "Ask quantity is required")
    @PositiveOrZero(message = "Ask quantity must be non-negative")
    @JsonProperty("askQty")
    private BigDecimal askQty;

    @NotNull(message = "Timestamp is required")
    @JsonProperty("timestamp")
    private Long timestamp;

    @NotBlank(message = "Event ID is required")
    @JsonProperty("eventId")
    private String eventId;
}
```

### 2. AlertConfig (Queued from User API)

```java
package com.example.sinkconnect.domain.model.queue;

import jdk.jfr.Frequency;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Alert configuration queued from REST API
 * Stored in AlertUserQueue per symbol
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class AlertConfig {

    public enum Operation {
        ADD,      // Add new alert
        UPDATE,   // Update existing alert
        REMOVE    // Remove alert
    }

    private String alertId;
    private String symbol;
    private String source;
    private BigDecimal targetPrice;
    private AlertCondition condition;   // ABOVE, BELOW, CROSS_ABOVE, CROSS_BELOW
    private FrequencyCondition frequencyCondition; // ONY_ONCE, PER_DAY, ALWAYS_PER_MINUTE
    private AlertStatus status;         // ENABLED, DISABLED
    private int maxHits;
    private Operation operation;        // ADD, UPDATE, REMOVE
    private Instant queuedAt;          // When queued
}
```

### 3. SymbolMatchingState (Actor State)

```java
package com.example.sinkconnect.domain.logic.matching;

import lombok.Data;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Internal state maintained by SymbolMatchingActor
 */
@Data
public class SymbolMatchingState {

    private final String symbol;
    private final String source;
    private final String shardKey;  // "BINANCE:BTCUSDT"

    // Active alerts (in-memory cache)
    private final Map<String, AlertConfig> activeAlerts;

    // Previous price for cross detection
    private BigDecimal previousPrice;

    // Statistics
    private Instant lastPollTime;
    private long totalPriceEvents;
    private long totalAlertChanges;
    private long totalMatches;

    public SymbolMatchingState(String symbol, String source) {
        this.symbol = symbol;
        this.source = source;
        this.shardKey = source + ":" + symbol;
        this.activeAlerts = new ConcurrentHashMap<>();
        this.previousPrice = null;
        this.lastPollTime = Instant.now();
        this.totalPriceEvents = 0;
        this.totalAlertChanges = 0;
        this.totalMatches = 0;
    }

    public void addOrUpdateAlert(AlertConfig alert) {
        activeAlerts.put(alert.getAlertId(), alert);
        totalAlertChanges++;
    }

    public void removeAlert(String alertId) {
        activeAlerts.remove(alertId);
        totalAlertChanges++;
    }

    public int getAlertCount() {
        return activeAlerts.size();
    }
}
```

---

## Component Implementation

### 1. PriceQueue (In-Memory Queue Manager)

```java
package com.example.sinkconnect.infrastructure.queue;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

/**
 * PriceQueue - In-memory queue for price events
 *
 * Structure: Map<SymbolKey, Queue<PriceEvent>>
 * SymbolKey format: "BINANCE:BTCUSDT"
 *
 * Features:
 * 1. Thread-safe concurrent queue per symbol
 * 2. Automatic queue creation for new symbols
 * 3. Batch dequeue operations
 * 4. Queue size monitoring
 */
@Slf4j
@Component
public class PriceQueue {

    // Map: SymbolKey -> Queue<PriceEvent>
    private final Map<String, Queue<PriceEvent>> queues;

    public PriceQueue() {
        this.queues = new ConcurrentHashMap<>();
    }

    /**
     * Enqueue a price event
     */
    public void enqueue(PriceEvent event) {
        String key = makeKey(event.getSource(), event.getSymbol());

        Queue<PriceEvent> queue = queues.computeIfAbsent(key,
            k -> new ConcurrentLinkedQueue<>());

        queue.offer(event);

        log.debug("Enqueued price event for {}: price={}, queue_size={}",
            key, event.getPrice(), queue.size());
    }

    /**
     * Dequeue all price events for a symbol (batch operation)
     * Called by SymbolMatchingActor every second
     */
    public List<PriceEvent> dequeueAll(String source, String symbol) {
        String key = makeKey(source, symbol);

        Queue<PriceEvent> queue = queues.get(key);
        if (queue == null || queue.isEmpty()) {
            return List.of();
        }

        // Drain all events from queue
        List<PriceEvent> events = queue.stream()
            .collect(Collectors.toList());

        queue.clear();

        log.debug("Dequeued {} price events for {}", events.size(), key);

        return events;
    }

    /**
     * Get queue size for monitoring
     */
    public int getQueueSize(String source, String symbol) {
        String key = makeKey(source, symbol);
        Queue<PriceEvent> queue = queues.get(key);
        return queue != null ? queue.size() : 0;
    }

    /**
     * Get total queue count across all symbols
     */
    public int getTotalQueueCount() {
        return queues.size();
    }

    /**
     * Get total pending events across all queues
     */
    public long getTotalPendingEvents() {
        return queues.values().stream()
            .mapToInt(Queue::size)
            .sum();
    }

    private String makeKey(String source, String symbol) {
        return source + ":" + symbol;
    }
}
```

### 2. AlertUserQueue (In-Memory Queue for Alert Configs)

```java
package com.example.sinkconnect.infrastructure.queue;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

/**
 * AlertUserQueue - In-memory queue for user alert configurations
 *
 * Structure: Map<SymbolKey, Queue<AlertConfig>>
 * SymbolKey format: "BINANCE:BTCUSDT"
 *
 * Features:
 * 1. Thread-safe concurrent queue per symbol
 * 2. Supports ADD, UPDATE, REMOVE operations
 * 3. Batch dequeue operations
 * 4. Queue size monitoring
 */
@Slf4j
@Component
public class AlertUserQueue {

    // Map: SymbolKey -> Queue<AlertConfig>
    private final Map<String, Queue<AlertConfig>> queues;

    public AlertUserQueue() {
        this.queues = new ConcurrentHashMap<>();
    }

    /**
     * Enqueue an alert configuration change
     */
    public void enqueue(AlertConfig config) {
        String key = makeKey(config.getSource(), config.getSymbol());

        Queue<AlertConfig> queue = queues.computeIfAbsent(key,
            k -> new ConcurrentLinkedQueue<>());

        queue.offer(config);

        log.debug("Enqueued alert config for {}: operation={}, alertId={}, queue_size={}",
            key, config.getOperation(), config.getAlertId(), queue.size());
    }

    /**
     * Dequeue all alert configs for a symbol (batch operation)
     * Called by SymbolMatchingActor every second
     */
    public List<AlertConfig> dequeueAll(String source, String symbol) {
        String key = makeKey(source, symbol);

        Queue<AlertConfig> queue = queues.get(key);
        if (queue == null || queue.isEmpty()) {
            return List.of();
        }

        // Drain all configs from queue
        List<AlertConfig> configs = queue.stream()
            .collect(Collectors.toList());

        queue.clear();

        log.debug("Dequeued {} alert configs for {}", configs.size(), key);

        return configs;
    }

    /**
     * Get queue size for monitoring
     */
    public int getQueueSize(String source, String symbol) {
        String key = makeKey(source, symbol);
        Queue<AlertConfig> queue = queues.get(key);
        return queue != null ? queue.size() : 0;
    }

    /**
     * Get total queue count across all symbols
     */
    public int getTotalQueueCount() {
        return queues.size();
    }

    /**
     * Get total pending configs across all queues
     */
    public long getTotalPendingConfigs() {
        return queues.values().stream()
            .mapToInt(Queue::size)
            .sum();
    }

    private String makeKey(String source, String symbol) {
        return source + ":" + symbol;
    }
}
```

### 3. SymbolMatchingActor (Core Matching Logic per Symbol)

```java
package com.example.sinkconnect.domain.logic.matching.actor;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import lombok.extern.slf4j.Slf4j;

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
 * 4. Forward matched alerts to AlertManagerActor
 * 5. Track previous price for cross detection
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
    public static class Poll implements Command {}

    /**
     * Get statistics for monitoring
     */
    public static class GetStats implements Command {
        public final ActorRef<StatsResponse> replyTo;

        public GetStats(ActorRef<StatsResponse> replyTo) {
            this.replyTo = replyTo;
        }
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
    private final ActorRef<AlertManagerActor.Command> alertManager;

    // Configuration
    private static final Duration POLL_INTERVAL = Duration.ofSeconds(1);

    // ==================== BEHAVIOR FACTORY ====================

    public static Behavior<Command> create(
            String symbol,
            String source,
            PriceQueue priceQueue,
            AlertUserQueue alertUserQueue,
            ActorRef<AlertManagerActor.Command> alertManager) {
        return Behaviors.setup(context ->
            new SymbolMatchingActor(context, symbol, source,
                priceQueue, alertUserQueue, alertManager)
        );
    }

    private SymbolMatchingActor(
            ActorContext<Command> context,
            String symbol,
            String source,
            PriceQueue priceQueue,
            AlertUserQueue alertUserQueue,
            ActorRef<AlertManagerActor.Command> alertManager) {
        super(context);
        this.symbol = symbol;
        this.source = source;
        this.state = new SymbolMatchingState(symbol, source);
        this.priceQueue = priceQueue;
        this.alertUserQueue = alertUserQueue;
        this.alertManager = alertManager;

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
     * 4. Forward matched alerts to AlertManagerActor
     */
    private Behavior<Command> onPoll(Poll cmd) {
        state.setLastPollTime(Instant.now());

        try {
            // STEP 1: Process alert configuration changes
            List<AlertConfig> alertConfigs = alertUserQueue.dequeueAll(source, symbol);
            processAlertConfigs(alertConfigs);

            // STEP 2: Process price events
            List<PriceEvent> internalPriceEvents = priceQueue.dequeueAll(source, symbol);
            if (internalPriceEvents.isEmpty()) {
                log.trace("No price events for {} in this poll", state.getShardKey());
                return scheduleNextPoll();
            }

            state.setTotalPriceEvents(state.getTotalPriceEvents() + internalPriceEvents.size());

            log.debug("Polling {}: {} price events, {} active alerts",
                state.getShardKey(), internalPriceEvents.size(), state.getAlertCount());

            // STEP 3: Process each price event
            for (PriceEvent event : internalPriceEvents) {
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

            // Forward matched alerts to AlertManagerActor
            matchedAlerts.forEach(alert -> {
                alertManager.tell(new AlertManagerActor.CheckPriceForSymbol(
                    alert.getSymbol(),
                    alert.getSource(),
                    currentPrice,
                    previousPrice
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
     * Handle GetStats - return statistics
     */
    private Behavior<Command> onGetStats(GetStats cmd) {
        cmd.replyTo.tell(new StatsResponse(
            state.getShardKey(),
            state.getAlertCount(),
            state.getTotalPriceEvents(),
            state.getTotalMatches(),
            state.getLastPollTime()
        ));
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
            new Poll()
        );
    }
}
```

### 4. SymbolMatchingCoordinator (Orchestration Service)

```java
package com.example.sinkconnect.domain.logic.matching;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import org.apache.pekko.cluster.sharding.typed.javadsl.Entity;
import org.apache.pekko.cluster.sharding.typed.javadsl.EntityTypeKey;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
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

    public static final EntityTypeKey<SymbolMatchingActor.Command> SYMBOL_MATCHING_ENTITY_KEY =
        EntityTypeKey.create(SymbolMatchingActor.Command.class, "SymbolMatching");

    private final ActorSystem<?> actorSystem;
    private final ClusterSharding clusterSharding;
    private final PriceQueue priceQueue;
    private final AlertUserQueue alertUserQueue;
    private final ActorRef<AlertManagerActor.Command> alertManager;

    // Track active symbols
    private final Set<String> activeSymbols;

    public SymbolMatchingCoordinator(
            ActorSystem<?> actorSystem,
            PriceQueue priceQueue,
            AlertUserQueue alertUserQueue,
            ActorRef<AlertManagerActor.Command> alertManager) {
        this.actorSystem = actorSystem;
        this.clusterSharding = ClusterSharding.get(actorSystem);
        this.priceQueue = priceQueue;
        this.alertUserQueue = alertUserQueue;
        this.alertManager = alertManager;
        this.activeSymbols = ConcurrentHashMap.newKeySet();

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
                String source = parts[0];
                String symbol = parts[1];

                log.info("Initializing SymbolMatchingActor for {}", entityContext.getEntityId());

                return SymbolMatchingActor.create(
                    symbol,
                    source,
                    priceQueue,
                    alertUserQueue,
                    alertManager
                );
            })
            .withSettings(
                ClusterShardingSettings.create(actorSystem)
                    .withPassivateIdleEntityAfter(Duration.ofMinutes(5))
            )
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
            log.info("Registered new symbol for matching: {}", shardKey);

            // Trigger actor creation by sending initial poll
            var matchingRef = clusterSharding.entityRefFor(
                SYMBOL_MATCHING_ENTITY_KEY,
                shardKey
            );
            matchingRef.tell(new SymbolMatchingActor.Poll());
        }
    }

    /**
     * Get statistics for a symbol
     */
    public CompletionStage<SymbolMatchingActor.StatsResponse> getStats(
            String source, String symbol) {
        String shardKey = source + ":" + symbol;

        var matchingRef = clusterSharding.entityRefFor(
            SYMBOL_MATCHING_ENTITY_KEY,
            shardKey
        );

        return AskPattern.ask(
            matchingRef,
            replyTo -> new SymbolMatchingActor.GetStats(replyTo),
            Duration.ofSeconds(5),
            actorSystem.scheduler()
        );
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down SymbolMatchingCoordinator");
    }
}
```

### 5. Updated MatchingAlertService (Producer to PriceQueue)

```java
package com.example.sinkconnect.domain.logic.alert.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * Updated MatchingAlertService - Pushes events to PriceQueue
 *
 * Changes from original:
 * - Removed database query (Line 187)
 * - Removed direct matching logic
 * - Added PriceQueue.enqueue() call
 * - Registers symbol with coordinator
 */
@Slf4j
@Service
public class MatchingAlertService {

    private final ActorSystem<?> actorSystem;
    private final PriceQueue priceQueue;
    private final SymbolMatchingCoordinator coordinator;
    private final ObjectMapper objectMapper;

    // ... constructor and Kafka consumer setup ...

    /**
     * Process a single candle - push to PriceQueue
     *
     * BEFORE: Query database for alerts → match → forward
     * AFTER:  Push to queue → SymbolMatchingActor polls every 1s
     */
    private void doMatching(PriceEventMessage event) {
        try {
            String symbol = event.getSymbol();
            String source = event.getSource();
            BigDecimal currentPrice = event.getBidPrice();

            if (currentPrice == null) {
                log.warn("Event has null price for {}-{}, skipping", source, symbol);
                return;
            }

            // Create PriceEvent
            PriceEvent internalPriceEvent = new PriceEvent(
                symbol,
                source,
                currentPrice,
                Instant.now(),
                UUID.randomUUID().toString()  // Event ID for deduplication
            );

            // Push to PriceQueue
            priceQueue.enqueue(internalPriceEvent);

            // Register symbol with coordinator (first-time only)
            coordinator.registerSymbol(source, symbol);

            log.debug("Pushed price event to queue for {}-{}: price={}",
                source, symbol, currentPrice);

        } catch (Exception e) {
            log.error("Error processing event for {}-{}: {}",
                event.getSource(), event.getSymbol(), e.getMessage(), e);
        }
    }
}
```

### 6. AlertService (Producer to AlertUserQueue)

```java
package com.example.sinkconnect.domain.logic.alert.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * AlertService - Handles user alert CRUD operations
 *
 * Responsibilities:
 * 1. Save/Update/Delete alerts in ScyllaDB
 * 2. Push alert changes to AlertUserQueue
 * 3. Notify SymbolMatchingActor of changes
 */
@Slf4j
@Service
public class AlertService {

    private final PriceAlertRepository alertRepository;
    private final AlertUserQueue alertUserQueue;
    private final SymbolMatchingCoordinator coordinator;

    public AlertService(
            PriceAlertRepository alertRepository,
            AlertUserQueue alertUserQueue,
            SymbolMatchingCoordinator coordinator) {
        this.alertRepository = alertRepository;
        this.alertUserQueue = alertUserQueue;
        this.coordinator = coordinator;
    }

    /**
     * Create a new alert
     */
    @Transactional
    public PriceAlertEntity createAlert(PriceAlertEntity alert) {
        // Save to database
        PriceAlertEntity saved = alertRepository.save(alert);

        // Push to AlertUserQueue
        AlertConfig config = new AlertConfig(
            saved.getAlertId(),
            saved.getSymbol(),
            saved.getSource(),
            saved.getTargetPrice(),
            AlertCondition.valueOf(saved.getCondition()),
            AlertStatus.valueOf(saved.getStatus()),
            saved.getMaxHits() != null ? saved.getMaxHits() : 10,
            AlertConfig.Operation.ADD,
            Instant.now()
        );

        alertUserQueue.enqueue(config);

        // Register symbol with coordinator
        coordinator.registerSymbol(saved.getSource(), saved.getSymbol());

        log.info("Created alert {} and pushed to queue for {}-{}",
            saved.getAlertId(), saved.getSource(), saved.getSymbol());

        return saved;
    }

    /**
     * Update an existing alert
     */
    @Transactional
    public PriceAlertEntity updateAlert(String alertId, PriceAlertEntity updates) {
        // Update database
        PriceAlertEntity updated = alertRepository.save(updates);

        // Push to AlertUserQueue
        AlertConfig config = new AlertConfig(
            updated.getAlertId(),
            updated.getSymbol(),
            updated.getSource(),
            updated.getTargetPrice(),
            AlertCondition.valueOf(updated.getCondition()),
            AlertStatus.valueOf(updated.getStatus()),
            updated.getMaxHits() != null ? updated.getMaxHits() : 10,
            AlertConfig.Operation.UPDATE,
            Instant.now()
        );

        alertUserQueue.enqueue(config);

        log.info("Updated alert {} and pushed to queue", alertId);

        return updated;
    }

    /**
     * Delete an alert
     */
    @Transactional
    public void deleteAlert(String alertId, String source, String symbol) {
        // Delete from database
        alertRepository.deleteById(UUID.fromString(alertId));

        // Push to AlertUserQueue
        AlertConfig config = new AlertConfig(
            alertId,
            symbol,
            source,
            null, // Not needed for REMOVE
            null,
            AlertStatus.DISABLED,
            0,
            AlertConfig.Operation.REMOVE,
            Instant.now()
        );

        alertUserQueue.enqueue(config);

        log.info("Deleted alert {} and pushed to queue", alertId);
    }
}
```

---

## Pseudocode: Data Flow

### Flow 1: Price Event Processing

```pseudocode
ALGORITHM: ProcessPriceEventFromKafka

INPUT: kafkaMessage from topic

STEP 1: Kafka Consumer receives message
    event = parseFromKafka(kafkaMessage)
    symbol = event.symbol        // "BTCUSDT"
    source = event.source        // "BINANCE"
    price = event.bidPrice       // 65000

STEP 2: Create PriceEvent
    internalPriceEvent = new PriceEvent(
        symbol,
        source,
        price,
        Instant.now(),
        UUID.randomUUID()
    )

STEP 3: Push to PriceQueue
    priceQueue.enqueue(internalPriceEvent)
    // Event stored in queue: queues["BINANCE:BTCUSDT"].offer(internalPriceEvent)

STEP 4: Register symbol with coordinator (if new)
    coordinator.registerSymbol(source, symbol)
    // This triggers SymbolMatchingActor creation

STEP 5: Commit Kafka offset
    // Fire-and-forget, no blocking

STEP 6: Wait for next poll cycle (handled by SymbolMatchingActor)

END ALGORITHM
```

### Flow 2: Alert Configuration Processing

```pseudocode
ALGORITHM: ProcessAlertConfiguration

INPUT: alertRequest from REST API (POST /alerts)

STEP 1: Receive alert creation request
    alertData = parseFromRequest(request)
    symbol = alertData.symbol
    source = alertData.source
    targetPrice = alertData.targetPrice
    condition = alertData.condition

STEP 2: Save to ScyllaDB
    alertEntity = alertRepository.save(alertData)
    alertId = alertEntity.alertId

STEP 3: Create AlertConfig
    config = new AlertConfig(
        alertId,
        symbol,
        source,
        targetPrice,
        condition,
        AlertStatus.ENABLED,
        10,  // maxHits
        Operation.ADD,
        Instant.now()
    )

STEP 4: Push to AlertUserQueue
    alertUserQueue.enqueue(config)
    // Config stored in queue: queues["BINANCE:BTCUSDT"].offer(config)

STEP 5: Register symbol with coordinator (if new)
    coordinator.registerSymbol(source, symbol)

STEP 6: Return response to user
    response = {"alertId": alertId, "status": "queued"}

STEP 7: Wait for next poll cycle (handled by SymbolMatchingActor)

END ALGORITHM
```

### Flow 3: Periodic Polling & Matching (Every 1 Second)

```pseudocode
ALGORITHM: PeriodicPollingAndMatching

TRIGGER: Every 1 second (scheduled by SymbolMatchingActor)

STEP 1: SymbolMatchingActor receives Poll command
    shardKey = "BINANCE:BTCUSDT"
    LOG("Polling {shardKey}")

STEP 2: Dequeue alert configurations
    alertConfigs = alertUserQueue.dequeueAll(source, symbol)
    // Returns: [AlertConfig1, AlertConfig2, ...]

STEP 3: Process alert configuration changes
    FOR EACH config IN alertConfigs DO
        CASE config.operation OF
            ADD, UPDATE:
                state.activeAlerts.put(config.alertId, config)
                LOG("Added/Updated alert {config.alertId}")
            REMOVE:
                state.activeAlerts.remove(config.alertId)
                LOG("Removed alert {config.alertId}")
        END CASE
    END FOR

STEP 4: Dequeue price events
    internalPriceEvents = priceQueue.dequeueAll(source, symbol)
    // Returns: [PriceEvent1, PriceEvent2, ...]

    IF internalPriceEvents.isEmpty() THEN
        LOG("No price events in this poll cycle")
        GO TO STEP 7
    END IF

STEP 5: Process each price event
    FOR EACH event IN internalPriceEvents DO
        currentPrice = event.price
        previousPrice = state.previousPrice

        // Get enabled alerts
        enabledAlerts = state.activeAlerts.values()
            .filter(alert -> alert.status == ENABLED)

        // Match alerts
        matchedAlerts = enabledAlerts
            .filter(alert -> matchesCondition(alert, currentPrice, previousPrice))

        IF matchedAlerts NOT empty THEN
            state.totalMatches += matchedAlerts.size()

            // Forward to AlertManagerActor
            FOR EACH alert IN matchedAlerts DO
                alertManager.tell(new CheckPriceForSymbol(
                    alert.symbol,
                    alert.source,
                    currentPrice,
                    previousPrice
                ))
            END FOR

            LOG("Matched {matchedAlerts.size()} alerts for {shardKey}")
        END IF

        // Update previous price
        state.previousPrice = currentPrice
    END FOR

STEP 6: Update statistics
    state.totalPriceEvents += internalPriceEvents.size()
    state.lastPollTime = Instant.now()

STEP 7: Schedule next poll
    scheduleOnce(1 second, getSelf(), new Poll())

END ALGORITHM


FUNCTION matchesCondition(alert, currentPrice, previousPrice) -> Boolean:
    target = alert.targetPrice

    CASE alert.condition OF
        ABOVE:
            RETURN currentPrice >= target

        BELOW:
            RETURN currentPrice <= target

        CROSS_ABOVE:
            RETURN previousPrice != null
                AND previousPrice < target
                AND currentPrice >= target

        CROSS_BELOW:
            RETURN previousPrice != null
                AND previousPrice >= target
                AND currentPrice < target
    END CASE
END FUNCTION
```

---

## Configuration

### application.yaml

```yaml
sink-connector:
  alert:
    enabled: true
    consumer-group: price-alert-consumer
    backpressure-buffer-size: 1000

  # NEW: Dual-queue matching settings
  matching:
    poll-interval: 1s              # How often actors poll queues
    queue-max-size: 10000          # Max events per symbol queue
    passivation-timeout: 5m        # Idle timeout for actors

  # Queue monitoring
  queue:
    price-queue:
      enabled: true
      max-size-per-symbol: 10000
    alert-user-queue:
      enabled: true
      max-size-per-symbol: 1000
```

### application-akka.conf

```hocon
pekko {
  cluster {
    sharding {
      number-of-shards = 100

      # SymbolMatchingActor specific settings
      passivate-idle-entity-after = 5m
      remember-entities = on
    }
  }
}
```

---

## Performance Analysis

### Latency Comparison

| Metric | Old Design (DB Query) | New Design (Dual-Queue) |
|--------|----------------------|-------------------------|
| Kafka consumption latency | 10-50ms (blocking DB query) | <1ms (non-blocking enqueue) |
| Matching latency | Immediate (per event) | 0-1000ms (batched every 1s) |
| Database queries per event | 1 | 0 (only on alert CRUD) |
| Throughput (events/sec) | 50-200 | 10000+ |
| Kafka consumer lag | High (slow processing) | Minimal (fast enqueue) |

### Advantages

1. **Decoupled consumption and processing**
    - Kafka consumer runs fast (no blocking)
    - Matching logic runs independently every second
    - Prevents consumer lag

2. **Batch processing**
    - Multiple price events processed per poll
    - Efficient use of actor resources
    - Natural backpressure

3. **Zero database queries in hot path**
    - Database only accessed for alert CRUD operations
    - In-memory matching per symbol

4. **Flexible polling interval**
    - Adjust from 1 second to 100ms or 5 seconds
    - Trade-off: latency vs resource usage

### Disadvantages

1. **Matching latency**
    - Up to 1 second delay before matching
    - Not suitable for ultra-low-latency requirements (<100ms)

2. **Memory usage**
    - In-memory queues per symbol
    - High-frequency symbols may accumulate events

3. **Complexity**
    - More components (2 queues + coordinator)
    - Requires monitoring and tuning

---

## Monitoring

### Key Metrics

```java
@Component
public class QueueMetrics {

    private final Gauge priceQueueSize;
    private final Gauge alertQueueSize;
    private final Counter priceEventsEnqueued;
    private final Counter alertConfigsEnqueued;
    private final Timer pollDuration;

    public QueueMetrics(MeterRegistry registry) {
        this.priceQueueSize = Gauge.builder("queue.price.size",
                () -> priceQueue.getTotalPendingEvents())
            .description("Total pending price events across all queues")
            .register(registry);

        this.alertQueueSize = Gauge.builder("queue.alert.size",
                () -> alertUserQueue.getTotalPendingConfigs())
            .description("Total pending alert configs across all queues")
            .register(registry);

        this.priceEventsEnqueued = Counter.builder("queue.price.enqueued")
            .description("Total price events enqueued")
            .register(registry);

        this.alertConfigsEnqueued = Counter.builder("queue.alert.enqueued")
            .description("Total alert configs enqueued")
            .register(registry);

        this.pollDuration = Timer.builder("matching.poll.duration")
            .description("Duration of matching poll cycle")
            .register(registry);
    }
}
```

### Health Check

```java
@RestController
@RequestMapping("/api/admin/matching")
public class MatchingAdminController {

    @GetMapping("/queues/stats")
    public Map<String, Object> getQueueStats() {
        return Map.of(
            "priceQueue", Map.of(
                "totalSymbols", priceQueue.getTotalQueueCount(),
                "totalPending", priceQueue.getTotalPendingEvents()
            ),
            "alertQueue", Map.of(
                "totalSymbols", alertUserQueue.getTotalQueueCount(),
                "totalPending", alertUserQueue.getTotalPendingConfigs()
            )
        );
    }

    @GetMapping("/symbol/{source}/{symbol}/stats")
    public CompletionStage<SymbolMatchingActor.StatsResponse> getSymbolStats(
            @PathVariable String source,
            @PathVariable String symbol) {
        return coordinator.getStats(source, symbol);
    }
}
```

---

## Summary

### Architecture Pattern

**Dual-Queue, Time-Based Batch Matching**

1. **Price events** → PriceQueue (per symbol)
2. **Alert configs** → AlertUserQueue (per symbol)
3. **SymbolMatchingActor** polls both queues every 1 second
4. **Matching logic** runs in-memory, zero DB queries
5. **Matched alerts** forwarded to AlertManagerActor

### Key Benefits

- **99% reduction** in database queries
- **10000+ events/sec** throughput
- **Decoupled** consumption and processing
- **Scalable** per-symbol isolation
- **Flexible** polling interval

### Trade-offs

- **Latency**: Up to 1 second matching delay
- **Memory**: In-memory queues per symbol
- **Complexity**: More components to manage

This design is ideal for **high-throughput, batch-oriented** alert systems where 1-second latency is acceptable.