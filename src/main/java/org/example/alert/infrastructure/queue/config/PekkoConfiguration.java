package org.example.alert.infrastructure.queue.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import jakarta.annotation.PreDestroy;

/**
 * Pekko configuration for Spring Boot
 * Sets up the ActorSystem with cluster sharding support
 */
@Slf4j
@Configuration
public class PekkoConfiguration {

    private ActorSystem<Void> coordinatorActorSystem;

    @Bean
    public ActorSystem<Void> actorSystem() {
        log.info("Initializing Pekko ActorSystem");

        // Load Pekko configuration
        Config config = ConfigFactory.load("application-pekko.conf")
            .withFallback(ConfigFactory.load());

        // Create actor system
        coordinatorActorSystem = ActorSystem.create(
            Behaviors.empty(),
            "AlertMatchingSystem",
            config
        );

        log.info("Pekko ActorSystem initialized successfully");

        return coordinatorActorSystem;
    }

    @PreDestroy
    public void shutdown() {
        if (coordinatorActorSystem != null) {
            log.info("Shutting down Pekko ActorSystem");
            coordinatorActorSystem.terminate();
        }
    }
}