package org.example.alert.domain.logic.consumer.actor;

import lombok.extern.slf4j.Slf4j;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.example.alert.domain.logic.matching.SymbolMatchingCoordinator;
import org.example.alert.domain.model.queue.InternalPriceEvent;
import org.example.alert.infrastructure.queue.PriceQueue;

@Slf4j
public class PriceEventConsumerActor extends AbstractBehavior<IngressActorCommand> {



    // ==================== DEPENDENCIES ====================

    private final PriceQueue priceQueue;
    private final SymbolMatchingCoordinator coordinator;


    // ==================== BEHAVIOR FACTORY ====================

    public static Behavior<IngressActorCommand> create(
            PriceQueue priceQueue,
            SymbolMatchingCoordinator coordinator) {
        return Behaviors.setup(context ->
            new PriceEventConsumerActor(context, priceQueue, coordinator)
        );
    }

    private PriceEventConsumerActor(
            ActorContext<IngressActorCommand> context,
            PriceQueue priceQueue,
            SymbolMatchingCoordinator coordinator) {
        super(context);
        this.priceQueue = priceQueue;
        this.coordinator = coordinator;

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

    private void pushToPriceQueue(InternalPriceEvent internalPriceEvent) {
        try {


            // Push to PriceQueue
            priceQueue.enqueue(internalPriceEvent);
            final String symbol = internalPriceEvent.getSymbol();
            final String source = internalPriceEvent.getSource();

            // Register symbol with coordinator (first-time only)
            coordinator.registerSymbol(source, symbol);

        } catch (Exception e) {
            log.error("Error pushing to PriceQueue: {}", e.getMessage(), e);
            throw e; // Re-throw to prevent commit
        }
    }
}
