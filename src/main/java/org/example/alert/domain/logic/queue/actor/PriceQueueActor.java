package org.example.alert.domain.logic.queue.actor;

import lombok.extern.slf4j.Slf4j;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.apache.pekko.cluster.sharding.typed.javadsl.EntityRef;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import org.example.alert.domain.model.ActorCommand;
import org.example.alert.domain.model.queue.InternalPriceEvent;

import java.time.Duration;
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
 *
 * Task 10 Enhancement:
 * - Integrates with PriceEventPersistenceActor for LevelDB persistence
 * - On Enqueue: persists data to LevelDB via ClusterSharding
 * - On Dequeue: fetches data from LevelDB and fills LinkedBlockingQueue
 */
@Slf4j
public class PriceQueueActor extends AbstractBehavior<ActorCommand> {

    // ==================== CONSTANTS ====================

    /**
     * Entity ID for persistence actor (single shard for simplicity)
     * Can be enhanced to multiple shards based on symbol/source
     */
    private static final String PERSISTENCE_ENTITY_ID = "price-events-0";

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
     * Internal command to handle persistence fetch response (Task 11)
     */
    private static class PersistenceFetchCompleted implements ActorCommand {
        public final ActorRef<ActorCommand> originalReplyTo;
        public final List<InternalPriceEvent> memoryEvents;
        public final List<InternalPriceEvent> persistedEvents;

        public PersistenceFetchCompleted(
            ActorRef<ActorCommand> originalReplyTo,
            List<InternalPriceEvent> memoryEvents,
            List<InternalPriceEvent> persistedEvents
        ) {
            this.originalReplyTo = originalReplyTo;
            this.memoryEvents = memoryEvents;
            this.persistedEvents = persistedEvents;
        }
    }

    /**
     * Internal command to handle persistence fetch failure (Task 11)
     */
    private static class PersistenceFetchFailed implements ActorCommand {
        public final ActorRef<ActorCommand> originalReplyTo;
        public final List<InternalPriceEvent> memoryEvents;
        public final Throwable error;

        public PersistenceFetchFailed(
            ActorRef<ActorCommand> originalReplyTo,
            List<InternalPriceEvent> memoryEvents,
            Throwable error
        ) {
            this.originalReplyTo = originalReplyTo;
            this.memoryEvents = memoryEvents;
            this.error = error;
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
     * Queue of price events (in-memory buffer)
     */
    private final LinkedBlockingQueue<InternalPriceEvent> queue;

    /**
     * Reference to persistence actor via ClusterSharding (Task 10)
     */
    private final EntityRef<ActorCommand> persistenceActor;

    // ==================== BEHAVIOR FACTORY ====================

    public static Behavior<ActorCommand> create() {
        return Behaviors.setup(PriceQueueActor::new);
    }

    public static Behavior<ActorCommand> create(ClusterSharding clusterSharding) {
        return Behaviors.setup(context -> new PriceQueueActor(context, clusterSharding));
    }

    private PriceQueueActor(ActorContext<ActorCommand> context) {
        super(context);
        this.queue = new LinkedBlockingQueue<>();

        // Get ClusterSharding and persistence actor reference (Task 10)
        ClusterSharding clusterSharding = ClusterSharding.get(context.getSystem());
        this.persistenceActor = clusterSharding.entityRefFor(
            PriceEventPersistenceActor.TYPE_KEY,
            PERSISTENCE_ENTITY_ID
        );

        log.info("PriceQueueActor started with persistence enabled");
    }

    private PriceQueueActor(ActorContext<ActorCommand> context, ClusterSharding clusterSharding) {
        super(context);
        this.queue = new LinkedBlockingQueue<>();

        // Get persistence actor reference (Task 10)
        this.persistenceActor = clusterSharding.entityRefFor(
            PriceEventPersistenceActor.TYPE_KEY,
            PERSISTENCE_ENTITY_ID
        );

        log.info("PriceQueueActor started with persistence enabled");
    }

    // ==================== MESSAGE HANDLERS ====================

    @Override
    public Receive<ActorCommand> createReceive() {
        return newReceiveBuilder()
            .onMessage(Enqueue.class, this::onEnqueue)
            .onMessage(Dequeue.class, this::onDequeue)
            .onMessage(GetQueueSize.class, this::onGetQueueSize)
            .onMessage(PriceEventPersistenceActor.FetchResponse.class, this::onFetchResponse)
            .onMessage(PersistenceFetchCompleted.class, this::onPersistenceFetchCompleted)
            .onMessage(PersistenceFetchFailed.class, this::onPersistenceFetchFailed)
            .build();
    }

    /**
     * Handle Enqueue command (Task 10)
     * - Adds price event to in-memory queue
     * - Persists to LevelDB via PriceEventPersistenceActor
     */
    private Behavior<ActorCommand> onEnqueue(Enqueue cmd) {
        try {
            // Add to in-memory queue
            queue.offer(cmd.priceEvent);

            // Persist to LevelDB via persistence actor (Task 10)
            persistenceActor.tell(new PriceEventPersistenceActor.Insert(
                cmd.priceEvent,
                getContext().getSelf()
            ));

            log.trace("Enqueued and persisting price event for {}:{} - price={} (queue size: {})",
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
     * Handle Dequeue command (Task 10)
     * - First dequeues from in-memory queue
     * - If in-memory queue is empty or insufficient, fetches from LevelDB persistence
     */
    private Behavior<ActorCommand> onDequeue(Dequeue cmd) {
        try {
            // Dequeue up to batchSize items from in-memory queue
            List<InternalPriceEvent> dequeued = new ArrayList<>();

            for (int i = 0; i < cmd.batchSize && !queue.isEmpty(); i++) {
                InternalPriceEvent event = queue.poll();
                if (event != null) {
                    dequeued.add(event);
                }
            }

            // If we have enough data from in-memory queue, return immediately
            if (dequeued.size() >= cmd.batchSize) {
                log.debug("Dequeued {} price events from memory (remaining: {})",
                    dequeued.size(), queue.size());
                cmd.replyTo.tell(new DequeueResponse(dequeued));
                return this;
            }

            // Otherwise, fetch from LevelDB persistence (Task 10 & Task 11)
            int remainingNeeded = cmd.batchSize - dequeued.size();

            log.debug("In-memory queue insufficient ({}/{}), fetching {} from LevelDB",
                dequeued.size(), cmd.batchSize, remainingNeeded);

            // Use context.ask to properly handle async response (Task 11 fix)
            getContext().ask(
                ActorCommand.class,
                persistenceActor,
                Duration.ofSeconds(5),
                replyTo -> new PriceEventPersistenceActor.Fetch(remainingNeeded, replyTo),
                (response, throwable) -> {
                    if (throwable != null) {
                        log.error("Error fetching from persistence: {}", throwable.getMessage(), throwable);
                        return new PersistenceFetchFailed(cmd.replyTo, dequeued, throwable);
                    }
                    if (response instanceof PriceEventPersistenceActor.FetchResponse fetchResp) {
                        return new PersistenceFetchCompleted(cmd.replyTo, dequeued, fetchResp.events);
                    } else {
                        log.warn("Unexpected response type from persistence: {}",
                            response != null ? response.getClass().getName() : "null");
                        return new PersistenceFetchFailed(cmd.replyTo, dequeued,
                            new IllegalStateException("Unexpected response type"));
                    }
                }
            );

        } catch (Exception e) {
            log.error("Error dequeuing price events: {}", e.getMessage(), e);
            // Send empty response on error
            cmd.replyTo.tell(new DequeueResponse(new ArrayList<>()));
        }

        return this;
    }

    /**
     * Handle FetchResponse from PriceEventPersistenceActor
     * Adds fetched events to in-memory queue
     */
    private Behavior<ActorCommand> onFetchResponse(PriceEventPersistenceActor.FetchResponse response) {
        if (response.events != null && !response.events.isEmpty()) {
            response.events.forEach(queue::offer);
            log.debug("Added {} events from persistence to in-memory queue (new size: {})",
                response.events.size(), queue.size());
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

    /**
     * Handle PersistenceFetchCompleted command (Task 11)
     * Combines memory events with persisted events and sends response
     */
    private Behavior<ActorCommand> onPersistenceFetchCompleted(PersistenceFetchCompleted cmd) {
        // Combine memory events and persisted events
        List<InternalPriceEvent> allEvents = new ArrayList<>(cmd.memoryEvents);
        allEvents.addAll(cmd.persistedEvents);

        log.debug("Dequeue completed: {} total events ({} from memory, {} from LevelDB)",
            allEvents.size(), cmd.memoryEvents.size(), cmd.persistedEvents.size());

        // Send response to original requester
        cmd.originalReplyTo.tell(new DequeueResponse(allEvents));

        return this;
    }

    /**
     * Handle PersistenceFetchFailed command (Task 11)
     * Returns what we have from memory when persistence fetch fails
     */
    private Behavior<ActorCommand> onPersistenceFetchFailed(PersistenceFetchFailed cmd) {
        log.warn("Persistence fetch failed, returning {} events from memory only: {}",
            cmd.memoryEvents.size(), cmd.error.getMessage());

        // Send response with only memory events
        cmd.originalReplyTo.tell(new DequeueResponse(cmd.memoryEvents));

        return this;
    }
}