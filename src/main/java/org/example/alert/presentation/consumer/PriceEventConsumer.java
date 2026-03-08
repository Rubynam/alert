package org.example.alert.presentation.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.kafka.ConsumerMessage;
import org.apache.pekko.kafka.ConsumerSettings;
import org.apache.pekko.kafka.Subscriptions;
import org.apache.pekko.kafka.javadsl.Consumer;
import org.apache.pekko.stream.OverflowStrategy;
import org.apache.pekko.stream.RestartSettings;
import org.apache.pekko.stream.javadsl.Keep;
import org.apache.pekko.stream.javadsl.RestartSource;
import org.apache.pekko.stream.javadsl.Sink;
import org.example.alert.application.port.InternalMessageTransform;
import org.example.alert.domain.logic.consumer.actor.IngressActorCommand;
import org.example.alert.domain.logic.consumer.actor.PriceEventConsumerActor;
import org.example.alert.domain.logic.matching.SymbolMatchingCoordinator;
import org.example.alert.domain.model.queue.PriceEventMessage;
import org.example.alert.infrastructure.queue.PriceQueue;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Objects;

@Service
@Slf4j
@RequiredArgsConstructor
public class PriceEventConsumer {

    private final ObjectMapper objectMapper;
    private final ActorSystem<?> actorSystem;
    private final PriceQueue priceQueue;
    private final SymbolMatchingCoordinator coordinator;
    private final InternalMessageTransform internalMessageTransform;

    private ActorRef<IngressActorCommand> consumerActor;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${sink-connector.topic}")
    private String inboundTopic;

    @Value("${sink-connector.alert.consumer-group:chart1m-alert-consumer}")
    private String consumerGroup;

    @Value("${sink-connector.alert.backpressure-buffer-size:1000}")
    private int backpressureBufferSize;

    /**
     * Initialize the consumer actor on application startup
     */
    @EventListener(ApplicationReadyEvent.class)
    void init() {
        log.info("Initializing PriceEventConsumerActor");

        // Create the consumer actor
        consumerActor = actorSystem.systemActorOf(
            PriceEventConsumerActor.create(priceQueue, coordinator),
            "priceEventConsumer",
            org.apache.pekko.actor.typed.DispatcherSelector.defaultDispatcher()
        );

        // Start consuming from Kafka
        startConsuming();
    }

    /**
     * Start consuming messages from Kafka
     */
    void startConsuming() {
        log.info("Starting Chart1m Kafka consumer for topic: {} with fault-tolerant restart strategy", inboundTopic);

        // Configure Kafka consumer settings with fault tolerance
        ConsumerSettings<String, String> consumerSettings =
                ConsumerSettings.create(actorSystem, new StringDeserializer(), new StringDeserializer())
                        .withBootstrapServers(bootstrapServers)
                        .withGroupId(consumerGroup)
                        // IMPORTANT: Use 'earliest' for recovery - consume from last committed offset
                        // This ensures messages in the gap time (during downtime) are processed
                        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                        .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
                        .withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500")
                        // Increase session timeout for resilience during slow processing
                        .withProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "60000")
                        .withProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "10000")
                        // Max time between polls - prevent consumer being kicked out during processing
                        .withProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "600000")
                        .withStopTimeout(Duration.ofSeconds(30));

        // Configure restart settings for automatic recovery
        RestartSettings restartSettings = RestartSettings.create(
                Duration.ofSeconds(3),    // minBackoff: initial delay before restart
                Duration.ofSeconds(30),   // maxBackoff: maximum delay between restarts
                0.2                       // randomFactor: add jitter to prevent thundering herd
        ).withMaxRestarts(10, Duration.ofMinutes(5)); // Max 10 restarts in 5 minutes window

        // Build Akka Streams pipeline with automatic restart on failure
        RestartSource.onFailuresWithBackoff(
                        restartSettings,
                        () -> {
                            log.info("(Re)starting Kafka consumer stream for topic: {}", inboundTopic);

                            return Consumer.committableSource(consumerSettings, Subscriptions.topics(inboundTopic))
                                    // Backpressure: Buffer up to N messages
                                    .buffer(backpressureBufferSize, OverflowStrategy.backpressure())
                                    .map(msg -> {
                                        try {
                                            String json = msg.record().value();
                                            PriceEventMessage candle = objectMapper.readValue(json, PriceEventMessage.class);
                                            return new MessageWithCommit(candle, msg.committableOffset());
                                        } catch (Exception e) {
                                            log.error("Failed to parse Candle1m from Kafka: {}", e.getMessage(), e);
                                            // Return null to filter out invalid messages
                                            return null;
                                        }
                                    })
                                    .filter(Objects::nonNull)
                                    .map(item -> new ImmutablePair<IngressActorCommand, ConsumerMessage.CommittableOffset>(internalMessageTransform.transform(item.event), item.committableOffset))

                                    // BEHAVIOR: Send to actor for processing
                                    .map(pair -> {
                                        var msg = pair.left;
                                        // Tell the actor to process this message
                                        consumerActor.tell(msg);
                                        // Return the committable offset for the stream
                                        return pair.right;
                                    })
                                    // Note: Commit is now handled by the actor
                                    .mapAsync(3, ConsumerMessage.Committable::commitJavadsl);
                        }
                )
                .watchTermination((notUsed, terminated) -> {
                    terminated.whenComplete((done, throwable) -> {
                        if (throwable != null) {
                            log.error("Kafka consumer stream terminated with error after max retries: {}",
                                    throwable.getMessage(), throwable);
                        } else {
                            log.info("Kafka consumer stream completed successfully");
                        }
                    });
                    return terminated;
                })
                // Run the stream
                .toMat(Sink.ignore(), Keep.right())
                .run(actorSystem);
    }

    private record MessageWithCommit(PriceEventMessage event, ConsumerMessage.CommittableOffset committableOffset) {
    }
}
