package org.example.alert.infrastructure.queue.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import org.apache.pekko.cluster.sharding.typed.javadsl.Entity;
import org.example.alert.domain.logic.queue.actor.PriceEventPersistenceActor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Pekko configuration for Spring Boot
 * Sets up the ActorSystem with cluster sharding support
 */
@Slf4j
@Configuration
public class PekkoConfiguration {

    ActorSystem<Void> coordinatorActorSystem;
    private ClusterSharding clusterSharding;

    @Bean
    public ActorSystem<Void> actorSystem() {
        log.info("Initializing Pekko ActorSystem");

        // Load Pekko configuration
        Config config = ConfigFactory.load("application-pekko")
                .resolve()
            .withFallback(ConfigFactory.load());

        // Create actor system
        this.coordinatorActorSystem = ActorSystem.create(
                Behaviors.empty(),
                "AlertMatchingSystem",
                config
        );

        // Initialize ClusterSharding
        initializeClusterSharding();

        log.info("Pekko ActorSystem initialized successfully");

        return coordinatorActorSystem;
    }

    /**
     * Initialize ClusterSharding for persistence actors (Task 10)
     */
    private void initializeClusterSharding() {
        log.info("Initializing ClusterSharding for PriceEventPersistenceActor");

        this.clusterSharding = ClusterSharding.get(coordinatorActorSystem);

        // Initialize PriceEventPersistenceActor sharding (Task 10)
        // Note: We just call init() to register the entity type with cluster sharding
        // The shard region will be accessed via ClusterSharding.entityRefFor() when needed
        clusterSharding.init(
            Entity.of(
                PriceEventPersistenceActor.TYPE_KEY,
                entityContext -> PriceEventPersistenceActor.create(entityContext.getEntityId())
            )
        );

        log.info("ClusterSharding initialized for PriceEventPersistenceActor");
    }

    /**
     * Get ClusterSharding instance
     */
    @Bean
    public ClusterSharding clusterSharding() {
        return clusterSharding;
    }

    @PreDestroy
    public void shutdown() {
        if (coordinatorActorSystem != null) {
            log.info("Shutting down Pekko ActorSystem");
            coordinatorActorSystem.terminate();
        }
    }
}