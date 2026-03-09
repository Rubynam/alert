package org.example.alert.domain.logic.matching.actor;

import lombok.extern.slf4j.Slf4j;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.example.alert.domain.model.ActorCommand;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * MatchingCoordinatorActor - Spawns and manages matching actors (Task 9)
 *
 * Responsibilities:
 * 1. Spawn MatchingActor every second
 * 2. Spawn additional actors if queue exceeds threshold
 * 3. Manage actor lifecycle
 */
@Slf4j
public class MatchingCoordinatorActor extends AbstractBehavior<ActorCommand> {

    // ==================== COMMANDS ====================

    /**
     * Command to spawn a matching actor
     */
    public static class SpawnMatchingActor implements ActorCommand {
        private static final SpawnMatchingActor INSTANCE = new SpawnMatchingActor();

        private SpawnMatchingActor() {}

        public static SpawnMatchingActor instance() {
            return INSTANCE;
        }
    }

    /**
     * Command to request additional actor spawning (sent by MatchingActor)
     */
    public static class RequestAdditionalActor implements ActorCommand {
        public final String source;
        public final String symbol;
        public final int queueSize;

        public RequestAdditionalActor(String source, String symbol, int queueSize) {
            this.source = source;
            this.symbol = symbol;
            this.queueSize = queueSize;
        }
    }

    // ==================== STATE ====================

    private final ActorSystem<?> actorSystem;
    private final ActorRef<ActorCommand> priceQueueActor;
    private final ActorRef<ActorCommand> userAlertQueueActor;
    private final ActorRef<ActorCommand> alertManagerActor;
    private final int batchSize;
    private final int spawnThreshold;

    private final AtomicInteger actorCounter = new AtomicInteger(0);

    private static final Duration SPAWN_INTERVAL = Duration.ofSeconds(1);

    // ==================== BEHAVIOR FACTORY ====================

    public static Behavior<ActorCommand> create(
            ActorSystem<?> actorSystem,
            ActorRef<ActorCommand> priceQueueActor,
            ActorRef<ActorCommand> userAlertQueueActor,
            ActorRef<ActorCommand> alertManagerActor,
            int batchSize,
            int spawnThreshold) {
        return Behaviors.setup(context ->
            new MatchingCoordinatorActor(
                context,
                actorSystem,
                priceQueueActor,
                userAlertQueueActor,
                alertManagerActor,
                batchSize,
                spawnThreshold
            )
        );
    }

    private MatchingCoordinatorActor(
            ActorContext<ActorCommand> context,
            ActorSystem<?> actorSystem,
            ActorRef<ActorCommand> priceQueueActor,
            ActorRef<ActorCommand> userAlertQueueActor,
            ActorRef<ActorCommand> alertManagerActor,
            int batchSize,
            int spawnThreshold) {
        super(context);
        this.actorSystem = actorSystem;
        this.priceQueueActor = priceQueueActor;
        this.userAlertQueueActor = userAlertQueueActor;
        this.alertManagerActor = alertManagerActor;
        this.batchSize = batchSize;
        this.spawnThreshold = spawnThreshold;

        // Schedule first spawn
        scheduleSpawn();

        log.info("MatchingCoordinatorActor started");
    }

    // ==================== MESSAGE HANDLERS ====================

    @Override
    public Receive<ActorCommand> createReceive() {
        return newReceiveBuilder()
            .onMessage(SpawnMatchingActor.class, this::onSpawnMatchingActor)
            .onMessage(RequestAdditionalActor.class, this::onRequestAdditionalActor)
            .build();
    }

    /**
     * Handle SpawnMatchingActor command - spawn a new matching actor every second
     */
    private Behavior<ActorCommand> onSpawnMatchingActor(SpawnMatchingActor cmd) {
        try {
            spawnMatchingActor();

        } catch (Exception e) {
            log.error("Error spawning matching actor: {}", e.getMessage(), e);
        }

        // Schedule next spawn
        scheduleSpawn();
        return this;
    }

    /**
     * Handle RequestAdditionalActor - spawn additional actor if queue exceeds threshold
     */
    private Behavior<ActorCommand> onRequestAdditionalActor(RequestAdditionalActor cmd) {
        try {
            if (cmd.queueSize >= spawnThreshold) {
                log.info("Queue size {} exceeds threshold {} for {}:{}, spawning additional actor",
                    cmd.queueSize, spawnThreshold, cmd.source, cmd.symbol);

                spawnMatchingActor();
            }

        } catch (Exception e) {
            log.error("Error spawning additional actor: {}", e.getMessage(), e);
        }

        return this;
    }

    // ==================== HELPER METHODS ====================

    /**
     * Spawn a new MatchingActor
     */
    private void spawnMatchingActor() {
        int actorId = actorCounter.incrementAndGet();
        String actorName = "matchingActor-" + actorId;

        ActorRef<ActorCommand> matchingActor = actorSystem.systemActorOf(
            MatchingActor.create(
                priceQueueActor,
                userAlertQueueActor,
                alertManagerActor,
                getContext().getSelf(),
                batchSize
            ),
            actorName,
            org.apache.pekko.actor.typed.DispatcherSelector.defaultDispatcher()
        );

        log.debug("Spawned MatchingActor: {}", actorName);
    }

    /**
     * Schedule the next spawn after 1 second
     */
    private void scheduleSpawn() {
        getContext().scheduleOnce(
            SPAWN_INTERVAL,
            getContext().getSelf(),
            SpawnMatchingActor.instance()
        );
    }
}