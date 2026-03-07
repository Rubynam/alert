package org.example.alert.application.config;

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

    private ActorSystem<Void> actorSystem;

    @Bean
    public ActorSystem<Void> actorSystem() {
        log.info("Initializing Pekko ActorSystem");

        // Load Pekko configuration
        Config config = ConfigFactory.load("application-pekko.conf")
            .withFallback(ConfigFactory.load());

        // Create actor system
        actorSystem = ActorSystem.create(
            Behaviors.empty(),
            "AlertMatchingSystem",
            config
        );

        log.info("Pekko ActorSystem initialized successfully");

        return actorSystem;
    }

    @PreDestroy
    public void shutdown() {
        if (actorSystem != null) {
            log.info("Shutting down Pekko ActorSystem");
            actorSystem.terminate();
        }
    }
}