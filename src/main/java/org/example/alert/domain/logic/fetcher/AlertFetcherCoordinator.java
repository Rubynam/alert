package org.example.alert.domain.logic.fetcher;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import org.apache.pekko.cluster.sharding.typed.javadsl.Entity;
import org.apache.pekko.cluster.sharding.typed.javadsl.EntityTypeKey;
import org.apache.pekko.actor.typed.DispatcherSelector;
import org.apache.pekko.cluster.typed.ClusterSingleton;
import org.apache.pekko.cluster.typed.SingletonActor;
import org.example.alert.domain.logic.fetcher.actor.ActorFetcherCommand;
import org.example.alert.domain.logic.fetcher.actor.ClusterBatchFetcherActor;
import org.example.alert.domain.logic.manager.actor.AlertManagerActor;
import org.example.alert.domain.logic.queue.actor.PriceQueueActor;
import org.example.alert.domain.logic.queue.actor.UserAlertQueueActor;
import org.example.alert.domain.model.ActorCommand;
import org.example.alert.infrastructure.persistence.repository.PriceAlertRepository;
import org.springframework.stereotype.Service;

/**
 * AlertFetcherCoordinator - Orchestrates cluster-aware batch fetching (Task 7 & 8)
 *
 * Responsibilities:
 * 1. Create queue actors (UserAlertQueueActor, PriceQueueActor) - Task 8
 * 2. Create ClusterBatchFetcherActor as cluster singleton - Task 7
 * 3. Distribute workload across cluster nodes automatically
 * 4. Use Pekko Streams batch operator for efficient processing
 * 5. Push data via Pekko messaging to queue actors
 *
 * Architecture:
 * - Queue Actors: Singleton actors managing in-memory queues
 * - Cluster Singleton: One ClusterBatchFetcherActor across cluster
 * - Workload Distribution: Each node fetches portion of total (e.g., 4 nodes: 200 each)
 * - Pekko Streams: Uses batch operator for reactive processing
 * - Scheduled: Runs every 1 second
 */
@Slf4j
@Service
public class AlertFetcherCoordinator {

    public static final EntityTypeKey<ActorFetcherCommand> ALERT_FETCHER_ENTITY_KEY =
        EntityTypeKey.create(ActorFetcherCommand.class, "AlertFetcher");

    private final ActorSystem<?> actorSystem;
    private final PriceAlertRepository repository;

    // Queue actors (Task 8)
    private final ActorRef<ActorCommand> userAlertQueueActor;
    private final ActorRef<ActorCommand> priceQueueActor;
    private final ActorRef<ActorCommand> alertManagerActor;

    private final ActorRef<ActorCommand> clusterBatchFetcher;


    public AlertFetcherCoordinator(
            ActorSystem<?> actorSystem,
            PriceAlertRepository repository) {
        this.actorSystem = actorSystem;
        this.repository = repository;

        // Create AlertManagerActor
        this.alertManagerActor = initializeAlertManagerActor();

        // Task 8: Create queue actors as singletons
        this.userAlertQueueActor = initializeUserAlertQueueActor();
        this.priceQueueActor = initializePriceQueueActor();

        // Task 7: Initialize ClusterBatchFetcherActor as cluster singleton
        this.clusterBatchFetcher = initializeClusterBatchFetcher();
    }

    /**
     * Getter for PriceQueueActor - used by PriceEventConsumer
     */
    public ActorRef<ActorCommand> getPriceQueueActor() {
        return priceQueueActor;
    }

    /**
     * Getter for UserAlertQueueActor - used by matching actors
     */
    public ActorRef<ActorCommand> getUserAlertQueueActor() {
        return userAlertQueueActor;
    }

    /**
     * Getter for AlertManagerActor - used by MatchingCoordinator
     */
    public ActorRef<ActorCommand> getAlertManagerActor() {
        return alertManagerActor;
    }

    /**
     * Initialize AlertManagerActor as singleton
     */
    private ActorRef<ActorCommand> initializeAlertManagerActor() {
        ActorRef<ActorCommand> managerActor = actorSystem.systemActorOf(
            AlertManagerActor.create(repository, ClusterSharding.get(actorSystem)),
            "alertManager",
            DispatcherSelector.defaultDispatcher()
        );

        log.info("AlertManagerActor initialized");
        return managerActor;
    }

    /**
     * Initialize UserAlertQueueActor as singleton (Task 8)
     */
    private ActorRef<ActorCommand> initializeUserAlertQueueActor() {
        ActorRef<ActorCommand> queueActor = actorSystem.systemActorOf(
            UserAlertQueueActor.create(),
            "userAlertQueue",
            DispatcherSelector.defaultDispatcher()
        );

        log.info("UserAlertQueueActor initialized");
        return queueActor;
    }

    /**
     * Initialize PriceQueueActor as singleton (Task 8)
     */
    private ActorRef<ActorCommand> initializePriceQueueActor() {
        ActorRef<ActorCommand> queueActor = actorSystem.systemActorOf(
            PriceQueueActor.create(),
            "priceQueue",
            DispatcherSelector.defaultDispatcher()
        );

        log.info("PriceQueueActor initialized");
        return queueActor;
    }

    /**
     * Initialize ClusterBatchFetcherActor as cluster singleton (Task 7 & 8)
     *
     * Configuration:
     * - Cluster Singleton: Ensures one instance per cluster
     * - Workload Distribution: Automatically distributes work across nodes
     * - Pekko Streams: Uses batch operator for processing
     * - Scheduled: Runs every 1 second
     * - Task 8: Sends to UserAlertQueueActor
     *
     * @return ActorRef for the cluster batch fetcher singleton
     */
    private ActorRef<ActorCommand> initializeClusterBatchFetcher() {
        ClusterSingleton singleton = ClusterSingleton.get(actorSystem);

        // Create singleton behavior - Task 8: pass userAlertQueueActor
        SingletonActor<ActorCommand> singletonActor = SingletonActor.of(
            ClusterBatchFetcherActor.create(repository, userAlertQueueActor),
            "ClusterBatchFetcher"
        );

        // Initialize the singleton
        ActorRef<ActorCommand> fetcherRef = singleton.init(singletonActor);

        log.info("ClusterBatchFetcherActor initialized as cluster singleton");

        return fetcherRef;
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down AlertFetcherCoordinator");
    }
}