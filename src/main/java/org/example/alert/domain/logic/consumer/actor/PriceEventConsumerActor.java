package org.example.alert.domain.logic.consumer.actor;

import lombok.extern.slf4j.Slf4j;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.example.alert.domain.logic.queue.actor.PriceQueueActor;
import org.example.alert.domain.model.ActorCommand;
import org.example.alert.domain.model.queue.InternalPriceEvent;

@Slf4j
public class PriceEventConsumerActor extends AbstractBehavior<IngressActorCommand> {



    // ==================== DEPENDENCIES ====================

    private final ActorRef<ActorCommand> priceQueueActor;

    // ==================== BEHAVIOR FACTORY ====================

    public static Behavior<IngressActorCommand> create(
            ActorRef<ActorCommand> priceQueueActor) {
        return Behaviors.setup(context ->
            new PriceEventConsumerActor(context, priceQueueActor)
        );
    }

    private PriceEventConsumerActor(
            ActorContext<IngressActorCommand> context,
            ActorRef<ActorCommand> priceQueueActor) {
        super(context);
        this.priceQueueActor = priceQueueActor;

        log.info("PriceEventConsumerActor started");
    }

    // ==================== MESSAGE HANDLERS ====================

    @Override
    public Receive<IngressActorCommand> createReceive() {
        return newReceiveBuilder()
            .onMessage(InternalPriceEvent.class, this::onProcessMessage)
            .build();
    }

    private Behavior<IngressActorCommand> onProcessMessage(InternalPriceEvent eventMessage) {
        pushToPriceQueue(eventMessage);
        return this;
    }

    /**
     * Push price event to PriceQueueActor via Pekko messaging (Task 8)
     */
    private void pushToPriceQueue(InternalPriceEvent internalPriceEvent) {
        try {
            // Send to PriceQueueActor via Pekko messaging
            priceQueueActor.tell(new PriceQueueActor.Enqueue(internalPriceEvent));

            log.trace("Sent price event to PriceQueueActor for {}:{}",
                internalPriceEvent.getSource(), internalPriceEvent.getSymbol());

        } catch (Exception e) {
            log.error("Error sending to PriceQueueActor: {}", e.getMessage(), e);
            throw e; // Re-throw to prevent commit
        }
    }
}
