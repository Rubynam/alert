package org.example.alert.domain.logic.fetcher.actor;

import lombok.extern.slf4j.Slf4j;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.apache.pekko.cluster.Member;
import org.apache.pekko.cluster.typed.Cluster;
import org.apache.pekko.stream.javadsl.Source;
import org.example.alert.domain.logic.queue.actor.UserAlertQueueActor;
import org.example.alert.domain.model.ActorCommand;
import org.example.alert.domain.model.enums.AlertCondition;
import org.example.alert.domain.model.enums.AlertStatus;
import org.example.alert.domain.model.enums.FrequencyCondition;
import org.example.alert.domain.model.queue.AlertConfig;
import org.example.alert.infrastructure.persistence.entity.PriceAlertEntity;
import org.example.alert.infrastructure.persistence.repository.PriceAlertRepository;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Slice;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * ClusterBatchFetcherActor - Cluster-aware batch fetcher using Pekko Streams
 *
 * Task 7 Requirements:
 * 1. Use Pekko Streams batch operator for efficient data handling
 * 2. Distribute workload across cluster nodes:
 *    - 4 nodes: Node1(1-200), Node2(201-400), Node3(401-600), Node4(601-800)
 * 3. Push data as reactive stream (Flux) via Pekko messaging
 * 4. Schedule execution every 1 second
 *
 * Architecture:
 * - Cluster-aware: Detects number of nodes and distributes work
 * - Batch processing: Uses Pekko Streams batch operator
 * - Reactive: Processes data as stream
 * - Scheduled: Runs every 1 second
 */
@Slf4j
public class ClusterBatchFetcherActor extends AbstractBehavior<ActorCommand> {

    // ==================== COMMANDS ====================

    /**
     * Command to trigger batch fetch
     */
    public static class FetchBatch implements ActorCommand {
        private static final FetchBatch INSTANCE = new FetchBatch();

        private FetchBatch() {}

        public static FetchBatch instance() {
            return INSTANCE;
        }
    }

    // ==================== STATE ====================

    private final Cluster cluster;
    private final PriceAlertRepository repository;
    private final ActorRef<ActorCommand> userAlertQueueActor;
    private final ActorRef<ActorCommand> self;

    // Configuration
    private static final Duration FETCH_INTERVAL = Duration.ofSeconds(1);
    private static final int BATCH_SIZE = 100; // Pekko Streams batch size

    // ==================== BEHAVIOR FACTORY ====================

    public static Behavior<ActorCommand> create(
            PriceAlertRepository repository,
            ActorRef<ActorCommand> userAlertQueueActor) {
        return Behaviors.setup(context ->
            new ClusterBatchFetcherActor(context, repository, userAlertQueueActor)
        );
    }

    private ClusterBatchFetcherActor(
            ActorContext<ActorCommand> context,
            PriceAlertRepository repository,
            ActorRef<ActorCommand> userAlertQueueActor) {
        super(context);
        this.cluster = Cluster.get(context.getSystem());
        this.repository = repository;
        this.userAlertQueueActor = userAlertQueueActor;
        this.self = context.getSelf();

        // Schedule first fetch
        scheduleFetch();

        log.info("ClusterBatchFetcherActor started");
    }

    // ==================== MESSAGE HANDLERS ====================

    @Override
    public Receive<ActorCommand> createReceive() {
        return newReceiveBuilder()
            .onMessage(FetchBatch.class, this::onFetchBatch)
            .build();
    }

    /**
     * Handle FetchBatch command - CORE BATCH FETCHING LOGIC
     *
     * Task 7 Implementation:
     * 1. Detect cluster members
     * 2. Calculate this node's offset and limit
     * 3. Fetch data from DB with pagination
     * 4. Use Pekko Streams batch operator
     * 5. Push to SymbolMatchingActors via Pekko messaging
     */
    private Behavior<ActorCommand> onFetchBatch(FetchBatch cmd) {
        try {
            // STEP 1: Get cluster members and calculate workload distribution
            Iterable<Member> members = cluster.state().getMembers();
            List<Member> sortedMembers = java.util.stream.StreamSupport.stream(members.spliterator(), false)
                .filter(m -> m.status().toString().equals("Up"))
                .sorted(Comparator.comparing(m -> m.address().toString()))
                .toList();

            int totalNodes = sortedMembers.size();
            if (totalNodes == 0) {
                log.warn("No active cluster members found");
                return scheduleNextFetch();
            }

            // Find this node's index
            Member selfMember = cluster.selfMember();
            int nodeIndex = sortedMembers.indexOf(selfMember);
            if (nodeIndex == -1) {
                log.warn("Self member not found in cluster members");
                return scheduleNextFetch();
            }

            // STEP 2: Calculate offset and limit for this node
            // Example: 4 nodes, 800 total
            // Node 0: offset=0, limit=200 (1-200)
            // Node 1: offset=200, limit=200 (201-400)
            // Node 2: offset=400, limit=200 (401-600)
            // Node 3: offset=600, limit=200 (601-800)
            long totalRecords = repository.countByStatus(AlertStatus.ENABLED.name());
            int limitPerNode = Math.toIntExact(totalRecords / totalNodes);
            int offset = nodeIndex * limitPerNode;
            int limit = limitPerNode;

            log.debug("Cluster batch fetch: node {}/{}, offset={}, limit={}",
                nodeIndex + 1, totalNodes, offset, limit);

            // STEP 3: Fetch alerts from database with pagination
            List<PriceAlertEntity> alerts = fetchAlertsWithPagination(offset, limit);

            if (alerts.isEmpty()) {
                log.trace("No alerts found for this node's range");
                return scheduleNextFetch();
            }

            log.debug("Fetched {} alerts for node {}/{}", alerts.size(), nodeIndex + 1, totalNodes);

            // STEP 4: Process with Pekko Streams batch operator
            processWithPekkoStreams(alerts);

        } catch (Exception e) {
            log.error("Error during cluster batch fetch: {}", e.getMessage(), e);
        }

        return scheduleNextFetch();
    }

    /**
     * Fetch alerts from database with pagination
     *
     * @param offset Starting offset for this node
     * @param limit Number of alerts to fetch
     * @return List of alerts
     */
    private List<PriceAlertEntity> fetchAlertsWithPagination(int offset, int limit) {
        try {
            // Calculate page number from offset
            int page = offset / limit;
            PageRequest pageRequest = PageRequest.of(page, limit);

            // Fetch ENABLED alerts
            Slice<PriceAlertEntity> alertSlice = repository
                .findByStatus(AlertStatus.ENABLED.name(), pageRequest);

            return alertSlice.getContent();

        } catch (Exception e) {
            log.error("Database fetch error: {}", e.getMessage());
            throw e;
        }
    }

    /**
     * Process alerts using Pekko Streams batch operator
     *
     * Task 7 Step 3: Use Pekko Streams to process data as reactive stream
     * and push to SymbolMatchingActors
     *
     * @param alerts List of alerts to process
     */
    private void processWithPekkoStreams(List<PriceAlertEntity> alerts) {
        // Create Source from list
        Source<PriceAlertEntity, org.apache.pekko.NotUsed> source = Source.from(alerts);

        // Use batch operator to group alerts
        source
            .batch(BATCH_SIZE, List::of, (list, alert) -> {
                list.add(alert);
                return list;
            })
            .runForeach(batch -> {
                // Process each batch
                for (PriceAlertEntity entity : batch) {
                    sendAlertToMatchingActor(entity);
                }
                log.trace("Processed batch of {} alerts", batch.size());
            }, getContext().getSystem());
    }

    /**
     * Send alert to UserAlertQueueActor via Pekko messaging (Task 8)
     *
     * @param entity Alert entity from database
     */
    private void sendAlertToMatchingActor(PriceAlertEntity entity) {
        try {
            // Convert to AlertConfig
            AlertConfig config = AlertConfig.builder()
                .alertId(entity.getAlertId())
                .symbol(entity.getSymbol())
                .source(entity.getSource())
                .targetPrice(entity.getTargetPrice())
                .condition(AlertCondition.valueOf(entity.getCondition()))
                .frequencyCondition(FrequencyCondition.valueOf(entity.getFrequencyCondition()))
                .status(AlertStatus.valueOf(entity.getStatus()))
                .hitCount(entity.getHitCount() != null ? entity.getHitCount() : 0)
                .maxHits(entity.getMaxHits() != null ? entity.getMaxHits() : 10)
                .operation(AlertConfig.Operation.ADD)
                .queuedAt(Instant.now())
                .build();

            // Send to UserAlertQueueActor via Pekko messaging
            userAlertQueueActor.tell(new UserAlertQueueActor.Enqueue(config));

            log.trace("Sent alert {} to UserAlertQueueActor for {}:{}",
                entity.getAlertId(), entity.getSource(), entity.getSymbol());

        } catch (Exception e) {
            log.error("Error sending alert {} to UserAlertQueueActor: {}",
                entity.getAlertId(), e.getMessage());
        }
    }

    // ==================== SCHEDULING ====================

    /**
     * Schedule the next batch fetch after 1 second
     */
    private Behavior<ActorCommand> scheduleNextFetch() {
        scheduleFetch();
        return this;
    }

    /**
     * Schedule fetch command every 1 second (cron-like)
     */
    private void scheduleFetch() {
        getContext().scheduleOnce(
            FETCH_INTERVAL,
            self,
            FetchBatch.instance()
        );
    }
}