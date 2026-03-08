package org.example.alert.domain.logic.queue.actor;

import lombok.extern.slf4j.Slf4j;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.example.alert.domain.model.ActorCommand;
import org.example.alert.domain.model.queue.InternalPriceEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * PriceQueueActor - Single actor managing price event queue
 *
 * Task 8 Requirements:
 * - Manages LinkedBlockingQueue<InternalPriceEvent>
 * - Implements enqueue and dequeue behaviors
 * - Receives price events from PriceEventConsumerActor via Pekko messaging
 * - Provides queue to MatchingActors for processing
 */
@Slf4j
public class PriceQueueActor extends AbstractBehavior<ActorCommand> {

    // ==================== COMMANDS ====================

    /**
     * Command to enqueue a price event
     */
    public static class Enqueue implements ActorCommand {
        public final InternalPriceEvent priceEvent;

        public Enqueue(InternalPriceEvent priceEvent) {
            this.priceEvent = priceEvent;
        }
    }

    /**
     * Command to dequeue price events (Task 9.1)
     */
    public static class Dequeue implements ActorCommand {
        public final int batchSize;
        public final ActorRef<ActorCommand> replyTo;

        public Dequeue(int batchSize, ActorRef<ActorCommand> replyTo) {
            this.batchSize = batchSize;
            this.replyTo = replyTo;
        }
    }

    /**
     * Response with dequeued price events (Task 9.1)
     */
    public static class DequeueResponse implements ActorCommand {
        public final List<InternalPriceEvent> events;

        public DequeueResponse(List<InternalPriceEvent> events) {
            this.events = events;
        }
    }

    /**
     * Command to get current queue size
     */
    public static class GetQueueSize implements ActorCommand {
        private static final GetQueueSize INSTANCE = new GetQueueSize();

        private GetQueueSize() {}

        public static GetQueueSize instance() {
            return INSTANCE;
        }
    }

    // ==================== STATE ====================

    /**
     * Queue of price events
     */
    private final LinkedBlockingQueue<InternalPriceEvent> queue;

    // ==================== BEHAVIOR FACTORY ====================

    public static Behavior<ActorCommand> create() {
        return Behaviors.setup(PriceQueueActor::new);
    }

    private PriceQueueActor(ActorContext<ActorCommand> context) {
        super(context);
        this.queue = new LinkedBlockingQueue<>();
        log.info("PriceQueueActor started");
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
     * Adds price event to the queue
     */
    private Behavior<ActorCommand> onEnqueue(Enqueue cmd) {
        try {
            queue.offer(cmd.priceEvent);

            log.trace("Enqueued price event for {}:{} - price={} (queue size: {})",
                cmd.priceEvent.getSource(),
                cmd.priceEvent.getSymbol(),
                cmd.priceEvent.getPrice(),
                queue.size());

        } catch (Exception e) {
            log.error("Error enqueueing price event for {}:{}: {}",
                cmd.priceEvent.getSource(),
                cmd.priceEvent.getSymbol(),
                e.getMessage(), e);
        }

        return this;
    }

    /**
     * Handle Dequeue command (Task 9.1)
     * Dequeues up to batchSize price events and returns them to the sender
     */
    private Behavior<ActorCommand> onDequeue(Dequeue cmd) {
        try {
            // Dequeue up to batchSize items (or empty list if queue is empty)
            List<InternalPriceEvent> dequeued = new ArrayList<>();

            for (int i = 0; i < cmd.batchSize && !queue.isEmpty(); i++) {
                InternalPriceEvent event = queue.poll();
                if (event != null) {
                    dequeued.add(event);
                }
            }

            log.debug("Dequeued {} price events (remaining: {})",
                dequeued.size(), queue.size());

            // Task 9.1: Send response back to the sender
            cmd.replyTo.tell(new DequeueResponse(dequeued));

        } catch (Exception e) {
            log.error("Error dequeuing price events: {}", e.getMessage(), e);
            // Send empty response on error
            cmd.replyTo.tell(new DequeueResponse(new ArrayList<>()));
        }

        return this;
    }

    /**
     * Handle GetQueueSize command
     * Returns the current queue size
     */
    private Behavior<ActorCommand> onGetQueueSize(GetQueueSize cmd) {
        int size = queue.size();
        log.debug("Price queue size: {}", size);

        // Note: In real implementation, this would reply to the sender
        // For now, we just log it

        return this;
    }
}