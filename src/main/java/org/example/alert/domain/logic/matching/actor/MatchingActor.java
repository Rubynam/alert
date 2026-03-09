package org.example.alert.domain.logic.matching.actor;

import lombok.extern.slf4j.Slf4j;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.example.alert.domain.logic.manager.actor.AlertManagerActor;
import org.example.alert.domain.logic.queue.actor.PriceQueueActor;
import org.example.alert.domain.logic.queue.actor.UserAlertQueueActor;
import org.example.alert.domain.model.ActorCommand;
import org.example.alert.domain.model.enums.AlertCondition;
import org.example.alert.domain.model.queue.AlertConfig;
import org.example.alert.domain.model.queue.InternalPriceEvent;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * MatchingActor - Dequeues from queues and matches alerts (Task 9)
 *
 * Responsibilities:
 * 1. Dequeue price events from PriceQueueActor
 * 2. Dequeue alerts from UserAlertQueueActor based on source:symbol
 * 3. Match using ABOVE and BELOW conditions only
 * 4. Tell AlertManagerActor about matched alerts
 * 5. Request additional actors if queue size exceeds threshold
 *
 * Lifecycle:
 * - Spawned by MatchingCoordinatorActor
 * - Processes one batch and then stops
 */
@Slf4j
public class MatchingActor extends AbstractBehavior<ActorCommand> {

    // ==================== COMMANDS ====================

    /**
     * Command to start processing
     */
    public static class StartProcessing implements ActorCommand {
        private static final StartProcessing INSTANCE = new StartProcessing();

        private StartProcessing() {}

        public static StartProcessing instance() {
            return INSTANCE;
        }
    }

    /**
     * Response from PriceQueueActor with dequeued events
     */
    public static class PriceEventsDequeued implements ActorCommand {
        public final List<InternalPriceEvent> events;

        public PriceEventsDequeued(List<InternalPriceEvent> events) {
            this.events = events;
        }
    }

    // ==================== STATE ====================

    private final ActorRef<ActorCommand> priceQueueActor;
    private final ActorRef<ActorCommand> userAlertQueueActor;
    private final ActorRef<ActorCommand> alertManagerActor;
    private final ActorRef<ActorCommand> coordinatorActor;
    private final int batchSize;

    // ==================== BEHAVIOR FACTORY ====================

    public static Behavior<ActorCommand> create(
            ActorRef<ActorCommand> priceQueueActor,
            ActorRef<ActorCommand> userAlertQueueActor,
            ActorRef<ActorCommand> alertManagerActor,
            ActorRef<ActorCommand> coordinatorActor,
            int batchSize) {
        return Behaviors.setup(context ->
            new MatchingActor(
                context,
                priceQueueActor,
                userAlertQueueActor,
                alertManagerActor,
                coordinatorActor,
                batchSize
            )
        );
    }

    private MatchingActor(
            ActorContext<ActorCommand> context,
            ActorRef<ActorCommand> priceQueueActor,
            ActorRef<ActorCommand> userAlertQueueActor,
            ActorRef<ActorCommand> alertManagerActor,
            ActorRef<ActorCommand> coordinatorActor,
            int batchSize) {
        super(context);
        this.priceQueueActor = priceQueueActor;
        this.userAlertQueueActor = userAlertQueueActor;
        this.alertManagerActor = alertManagerActor;
        this.coordinatorActor = coordinatorActor;
        this.batchSize = batchSize;

        // Start processing immediately
        getContext().getSelf().tell(StartProcessing.instance());

        log.trace("MatchingActor started");
    }

    // ==================== MESSAGE HANDLERS ====================

    @Override
    public Receive<ActorCommand> createReceive() {
        return newReceiveBuilder()
            .onMessage(StartProcessing.class, this::onStartProcessing)
            .onMessage(PriceQueueActor.DequeueResponse.class, this::onPriceEventsDequeued)
            .onMessage(UserAlertQueueActor.DequeueResponse.class, this::onAlertsDequeued)
            .build();
    }

    /**
     * Start processing - request price events from PriceQueueActor (Task 9.1)
     */
    private Behavior<ActorCommand> onStartProcessing(StartProcessing cmd) {
        // Task 9.1: Request dequeue from PriceQueueActor with replyTo
        priceQueueActor.tell(new PriceQueueActor.Dequeue(batchSize, getContext().getSelf()));

        log.trace("Requested {} price events from PriceQueueActor", batchSize);

        // Wait for response
        return this;
    }

    /**
     * Handle price events dequeued (Task 9.1 - response from PriceQueueActor)
     */
    private Behavior<ActorCommand> onPriceEventsDequeued(PriceQueueActor.DequeueResponse response) {
        if (response.events.isEmpty()) {
            log.trace("No price events to process, stopping");
            return Behaviors.stopped();
        }

        log.debug("Processing {} price events", response.events.size());

        // For each price event, request matching alerts
        for (InternalPriceEvent event : response.events) {
            userAlertQueueActor.tell(
                new UserAlertQueueActor.Dequeue(event, batchSize,getContext().getSelf())
            );
        }

        return this;
    }

    /**
     * Handle alerts dequeued (would be sent by UserAlertQueueActor in full implementation)
     */
    private Behavior<ActorCommand> onAlertsDequeued(UserAlertQueueActor.DequeueResponse cmd) {
        if (cmd.getQueue().isEmpty()) { //TODO DUPLICATED PULL DATA FROM DATABASE
            return Behaviors.stopped();
        }


        // Check if we need to spawn additional actor
        if (cmd.getQueue().size() >= batchSize) {
            coordinatorActor.tell(
                new MatchingCoordinatorActor.RequestAdditionalActor(
                    cmd.getSource(),
                    cmd.getSymbol(),
                    cmd.getRemaining()
                )
            );
        }

        processMatching(cmd.getQueue(), cmd.getEvent());

        return this;
    }

    // ==================== HELPER METHODS ====================

    /**
     * Process matching between alerts and price
     * Task 9: Only support ABOVE and BELOW conditions
     */
    private void processMatching(LinkedBlockingQueue<AlertConfig> alerts, InternalPriceEvent priceEvent) {
        if (priceEvent == null) {
            // In full implementation, we'd have the price event here
            return;
        }

        BigDecimal currentPrice = priceEvent.getPrice();

        for (AlertConfig alert : alerts) {
            boolean matched = matchesCondition(alert, currentPrice);

            if (matched) {
                log.info("Alert matched: {} - condition: {} - target: {} - current: {}",
                    alert.getAlertId(), alert.getCondition(),
                    alert.getTargetPrice(), currentPrice);

                // Tell AlertManagerActor
                alertManagerActor.tell(new AlertManagerActor.AlertMatched(
                    alert.getAlertId(),
                    alert.getSymbol(),
                    alert.getSource(),
                    currentPrice,
                    null, //todo previousPrice not available in this simplified version
                    alert.getFrequencyCondition(),
                    alert.getHitCount(),
                    alert.getMaxHits()
                ));
            }else{
                //revertransfrom and push to queue
            }
        }
    }

    /**
     * Check if alert matches current price
     * Task 9: Only ABOVE and BELOW conditions supported
     */
    private boolean matchesCondition(AlertConfig alert, BigDecimal currentPrice) {
        BigDecimal targetPrice = alert.getTargetPrice();

        return switch (alert.getCondition()) {
            case ABOVE -> currentPrice.compareTo(targetPrice) >= 0;
            case BELOW -> currentPrice.compareTo(targetPrice) <= 0;
            default -> {
                log.warn("Unsupported condition: {} (Task 9 only supports ABOVE and BELOW)",
                    alert.getCondition());
                yield false;
            }
        };
    }
}