package org.example.alert.infrastructure.queue;

import lombok.extern.slf4j.Slf4j;
import org.example.alert.domain.model.queue.InternalPriceEvent;
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
    private final Map<String, Queue<InternalPriceEvent>> queues;

    public PriceQueue() {
        this.queues = new ConcurrentHashMap<>();
    }

    /**
     * Enqueue a price event
     */
    public void enqueue(InternalPriceEvent event) {
        String key = makeKey(event.getSource(), event.getSymbol());

        Queue<InternalPriceEvent> queue = queues.computeIfAbsent(key,
            k -> new ConcurrentLinkedQueue<>());

        queue.offer(event);

        log.debug("Enqueued price event for {}: price={}, queue_size={}",
            key, event.getPrice(), queue.size());
    }

    /**
     * Dequeue all price events for a symbol (batch operation)
     * Called by SymbolMatchingActor every second
     */
    public List<InternalPriceEvent> dequeueAll(String source, String symbol) {
        String key = makeKey(source, symbol);

        Queue<InternalPriceEvent> queue = queues.get(key);
        if (queue == null || queue.isEmpty()) {
            return List.of();
        }

        // Drain all events from queue
        List<InternalPriceEvent> events = queue.stream()
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
        Queue<InternalPriceEvent> queue = queues.get(key);
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