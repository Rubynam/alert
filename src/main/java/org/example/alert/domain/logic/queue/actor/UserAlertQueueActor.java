package org.example.alert.domain.logic.queue.actor;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.example.alert.domain.model.ActorCommand;
import org.example.alert.domain.model.queue.AlertConfig;
import org.example.alert.domain.model.queue.InternalPriceEvent;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * UserAlertQueueActor - Single actor managing alert queues per symbol
 *
 * Task 8 Requirements:
 * - Manages Map<String, LinkedBlockingQueue<AlertConfig>> where key is "source:symbol"
 * - Implements enqueue and dequeue behaviors
 * - Receives alerts from ClusterBatchFetcherActor via Pekko messaging
 * - Provides queue size information for dynamic actor spawning
 */
@Slf4j
public class UserAlertQueueActor extends AbstractBehavior<ActorCommand> {

    // ==================== COMMANDS ====================

    /**
     * Command to enqueue an alert
     */
    public static class Enqueue implements ActorCommand {
        public final AlertConfig alertConfig;

        public Enqueue(AlertConfig alertConfig) {
            this.alertConfig = alertConfig;
        }
    }

    /**
     * Command to dequeue alerts for a specific source:symbol
     */
    public static class Dequeue implements ActorCommand {
        final String source;
        final String symbol;
        final int batchSize;
        final InternalPriceEvent event;
        final ActorRef<ActorCommand> replyTo;


        public Dequeue(InternalPriceEvent event, int batchSize, ActorRef<ActorCommand> replyTo) {
            this.source = event.getSource();
            this.symbol = event.getSymbol();
            this.batchSize = batchSize;
            this.event = event;
            this.replyTo = replyTo;
        }
    }

    @Getter
    public static class DequeueResponse implements ActorCommand {
        private InternalPriceEvent event;
        private LinkedBlockingQueue<AlertConfig> queue;
        private final int remaining;
        private final String source;
        private final String symbol;
        public DequeueResponse(InternalPriceEvent event, LinkedBlockingQueue<AlertConfig> queue, int remaining, String source, String symbol) {
            this.event = event;
            this.queue = queue;
            this.remaining = remaining;
            this.source = source;
            this.symbol = symbol;
        }
    }

    /**
     * Command to get queue size for a specific source:symbol
     */
    public static class GetQueueSize implements ActorCommand {
        public final String source;
        public final String symbol;

        public GetQueueSize(String source, String symbol) {
            this.source = source;
            this.symbol = symbol;
        }
    }

    // ==================== STATE ====================

    /**
     * Map of queues: key = "source:symbol", value = queue of alerts
     */
    private final Map<String, LinkedBlockingQueue<AlertConfig>> mapQueue;

    // ==================== BEHAVIOR FACTORY ====================

    public static Behavior<ActorCommand> create() {
        return Behaviors.setup(UserAlertQueueActor::new);
    }

    private UserAlertQueueActor(ActorContext<ActorCommand> context) {
        super(context);
        this.mapQueue = new ConcurrentHashMap<>();
        log.info("UserAlertQueueActor started");
    }

    // ==================== MESSAGE HANDLERS ====================

    @Override
    public Receive<ActorCommand> createReceive() {
        return newReceiveBuilder()
            .onMessage(Enqueue.class, this::onEnqueue)
            .onMessage(Dequeue.class, this::onDequeue)
            .onMessage(GetQueueSize.class, this::onGetQueueSize)
            .build();
    }

    /**
     * Handle Enqueue command
     * Adds alert to the queue for the corresponding source:symbol
     */
    private Behavior<ActorCommand> onEnqueue(Enqueue cmd) {
        try {
            String key = buildKey(cmd.alertConfig.getSource(), cmd.alertConfig.getSymbol());

            // Get or create queue for this key
            LinkedBlockingQueue<AlertConfig> queue = mapQueue.computeIfAbsent(
                key,
                k -> new LinkedBlockingQueue<>()
            );

            // Add to queue
            queue.offer(cmd.alertConfig);

            log.trace("Enqueued alert {} for {} (queue size: {})",
                cmd.alertConfig.getAlertId(), key, queue.size());

        } catch (Exception e) {
            log.error("Error enqueueing alert {}: {}",
                cmd.alertConfig.getAlertId(), e.getMessage(), e);
        }

        return this;
    }

    /**
     * Handle Dequeue command
     * Dequeues up to batchSize alerts for the specified source:symbol
     */
    private Behavior<ActorCommand> onDequeue(Dequeue cmd) {
        try {
            String key = buildKey(cmd.source, cmd.symbol);
            LinkedBlockingQueue<AlertConfig> queue = mapQueue.get(key);

            if (queue == null || queue.isEmpty()) {
                log.trace("No alerts in queue for {}", key);
                return this;
            }

            // Dequeue up to batchSize items
            LinkedBlockingQueue<AlertConfig> dequeued = new LinkedBlockingQueue<>();
            for (int i = 0; i < cmd.batchSize && !queue.isEmpty(); i++) {
                AlertConfig alert = queue.poll();
                if (alert != null) {
                    dequeued.add(alert);
                }
            }
            int remaining = dequeued.size();
            String source = cmd.source;
            String symbol = cmd.symbol;

            log.debug("Dequeued {} alerts for {} (remaining: {})",
                dequeued.size(), key, queue.size());

            cmd.replyTo.tell(new DequeueResponse(cmd.event,dequeued,remaining,source,symbol));

        } catch (Exception e) {
            log.error("Error dequeuing alerts for {}:{}: {}",
                cmd.source, cmd.symbol, e.getMessage(), e);
        }

        return this;
    }

    /**
     * Handle GetQueueSize command
     * Returns the current queue size for the specified source:symbol
     */
    private Behavior<ActorCommand> onGetQueueSize(GetQueueSize cmd) {
        String key = buildKey(cmd.source, cmd.symbol);
        LinkedBlockingQueue<AlertConfig> queue = mapQueue.get(key);

        int size = (queue != null) ? queue.size() : 0;

        log.debug("Queue size for {}: {}", key, size);

        // Note: In real implementation, this would reply to the sender
        // For now, we just log it

        return this;
    }

    // ==================== HELPER METHODS ====================

    /**
     * Build queue key from source and symbol
     */
    private String buildKey(String source, String symbol) {
        return source + ":" + symbol;
    }
}