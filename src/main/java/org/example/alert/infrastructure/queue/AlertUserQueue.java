package org.example.alert.infrastructure.queue;

import lombok.extern.slf4j.Slf4j;
import org.example.alert.domain.model.queue.AlertConfig;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

/**
 * AlertUserQueue - In-memory queue for user alert configurations
 * -------------------------------------
 * Structure: Map<SymbolKey, Queue<AlertConfig>>
 * SymbolKey format: "BINANCE:BTCUSDT"
 * -------------------------------------
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
        List<AlertConfig> configs = new ArrayList<>(queue);

        queue.clear();

        log.debug("Dequeued {} alert configs for {}", configs.size(), key);

        return configs;
    }

    private String makeKey(String source, String symbol) {
        return source + ":" + symbol;
    }
}