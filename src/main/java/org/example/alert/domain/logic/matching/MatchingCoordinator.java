package org.example.alert.domain.logic.matching;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.DispatcherSelector;
import org.example.alert.domain.logic.fetcher.AlertFetcherCoordinator;
import org.example.alert.domain.logic.matching.actor.MatchingCoordinatorActor;
import org.example.alert.domain.model.ActorCommand;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * MatchingCoordinator - Coordinates matching actors (Task 9)
 *
 * Responsibilities:
 * 1. Spawn MatchingActors every second
 * 2. Spawn additional actors if UserAlertQueue exceeds threshold
 * 3. Distribute actors across cluster nodes
 *
 * Architecture:
 * - Scheduled spawning: Every 1 second
 * - Dynamic spawning: Based on queue size threshold
 * - Cluster-aware: Actors can run on different nodes
 */
@Slf4j
@Service
public class MatchingCoordinator {

    private final ActorSystem<?> actorSystem;
    private final ActorRef<ActorCommand> priceQueueActor;
    private final ActorRef<ActorCommand> userAlertQueueActor;
    private final ActorRef<ActorCommand> alertManagerActor;

    // Configuration from application.yaml
    private final int batchSize;
    private final int spawnThreshold;

    // Coordinator actor
    private final ActorRef<ActorCommand> coordinatorActor;

    public MatchingCoordinator(
            ActorSystem<?> actorSystem,
            AlertFetcherCoordinator alertFetcherCoordinator,
            @Value("${alert.matching.batch-size:100}") int batchSize,
            @Value("${alert.matching.spawn-threshold:500}") int spawnThreshold) {
        this.actorSystem = actorSystem;
        this.priceQueueActor = alertFetcherCoordinator.getPriceQueueActor();
        this.userAlertQueueActor = alertFetcherCoordinator.getUserAlertQueueActor();
        this.alertManagerActor = alertFetcherCoordinator.getAlertManagerActor();
        this.batchSize = batchSize;
        this.spawnThreshold = spawnThreshold;

        // Create coordinator actor
        this.coordinatorActor = initializeCoordinatorActor();

        log.info("MatchingCoordinator initialized with batchSize={}, spawnThreshold={}",
            batchSize, spawnThreshold);
    }

    /**
     * Initialize the coordinator actor that spawns matching actors
     */
    private ActorRef<ActorCommand> initializeCoordinatorActor() {
        ActorRef<ActorCommand> coordinator = actorSystem.systemActorOf(
            MatchingCoordinatorActor.create(
                actorSystem,
                priceQueueActor,
                userAlertQueueActor,
                alertManagerActor,
                batchSize,
                spawnThreshold
            ),
            "matchingCoordinator",
            DispatcherSelector.defaultDispatcher()
        );

        log.info("MatchingCoordinatorActor initialized");
        return coordinator;
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down MatchingCoordinator");
    }
}