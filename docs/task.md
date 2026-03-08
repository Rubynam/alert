# Background
 - Read the dual-queue-matching-design.md

## Task 1 [Init]
 - Setup implement Pekko for matching syste.
 - Setting  project must follow Domain Driven Design
 - Create a application-pekko.conf to setup pekko config.
 - *DO NOT*: You do not start Pekko Http. Let Springboot handle
   - Do not create unit test.
 
## Task 2 [Consumer setting]
 - Read PriceEventConsumer. You create a Actor to consume message from Kafka
 - This actor have 3 behaviors.
   - 1. Consume message
   - 2. Push data to PriceQueue
   - 3. After push to queue, message with commit
## Task 3 Sharded storage at absolute path
 - Background. You applied Apache Pekko, and applied ClusterShard
 - Requirement: You *must* set absoluate shard path at current path project, create a folder, named pekko_data
 - Action: *must* share shard data in internal network using akko protocol.
## Task 4; Connect Scylla DB mode master slave via springboot
 - Requirement: You *must* create connection to connect scylla DB
 - Action: add spring jpa starter cassandra to establish a connection to scylla db
 - Create a entity and repository as a persuade code below
```java
@Table("user_alert")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PriceAlertEntity {

    @PrimaryKey
    @Column("alert_id")
    @Builder.Default
    private String alertId = UUID.randomUUID().toString();

    @Column("symbol")
    private String symbol;

    @Column("source")
    private String source;

    @Column("target_price")
    private BigDecimal targetPrice;

    @Column("condition")
    private String condition;

    @Column("frequency_condition")
    private String frequencyCondition;

    @Column("status")
    private String status;

    @Column("hit_count")
    private Integer hitCount;

    @Column("max_hits")
    private Integer maxHits;

    @Column("created_at")
    private Instant createdAt;

    @Column("updated_at")
    private Instant updatedAt;
}
```
## Task 5: Create a isolated Actor for scheduling
 - Requirement: After Task 4 done, you create isolated Actor to fetch data user_alert from DB.
 - Action: Create many actor to fetch user_alert database based on source and symbol
   - A actor must have responsible for fetching user_alert based on source and symbol
   - Actors can share workload via cluster pekko
   - The maximum actor in a cluster is 600000 actors
   - Fetching strategy: Flow FIFO user_laert sort by desc. 
   - The batch size is 100 per actor.
   - Every second, actor call fetch data from DB
 - Action 2: After fetching sucess, actor push the whole data into AlertUserQueue.

## Task 6: Implement AlertManagerActor
- Requirement: After matched alert, you need update the new status to DB
- Action: 
  - For the entity in user_alert, you must update hitCount+=1. If entify have a frequency_condition which is ONLY_ONE, max_count =1 and you change the status to TRIGGERED. Other you must to update the status is TRIGGERED. and then invoke repo to update 
  - For the AlertConfig you need to update the operation is REMOVE, and send a signal to SymbolMatchingActor to remove this alert in queue.

## Task 6.1 Enhance the AlertMayerActor
 - Action: AlertMayerActor need to read the local queue in a local data shard.
   - You need to send a signal to SymbolMatchingActor via Pekko, and notify other shard if needed.
   - *DO NOT* implement stats to measure.

## Task 6.2 Enhance the AlertFetcherActor
- Action: AlertFetcherActor add data to queue
   - You need to send a signal to SymbolMatchingActor via Pekko, and notify other shard if needed.
   - *DO NOT* implement stats to measure.

## Task 6.3 AlertFetcherCoordinator does not create
- Action AlertFetcherCoordinator amd AlertFetcherActor, you need to create a varity actor based on logic
  - 1. PriceEventConsumer consumerActor need to tell with input symbol and source to AlertFetcherCoordinator to create actor per symbol and call fetch data and push queue now.

## Task 7: Enhance AlertFetcherActor
- Requirement: Use apply apache pekko BATCH to handle batch data in AlertFetcherActor (https://pekko.apache.org/docs/pekko/current/stream/operators/Source-or-Flow/batch.html)
- Action:
  - Step 1: Create a a schedule batch to fetch data from DB. Fetching's size need to have a limit. 
    - Step 1.1. Workload for fetching size must to share across PEKKO node in cluster. For example: A CLuster has 4 nodes, 1 nodes fetch the first 200/800 data size, node 2 fetch from 201/800 to 400, node 3 fetch 401 to 600 and last fetch to 600-800
  - Step 2: Schedule batch need to share across nodes in Pekko CLuster
  - Step 3: After fetch data push Data as Flux to Queue via Pekko Actor.
  - Step 4: Set up cron job run every second

## Task 8: Enhance queue
- Requirement: Currently, PriceQueue,UserAlertActor , saved in memory, this can lead to OOM java
- Action:
  - Create UserAlertQueueActor as a single actor. This Actor mange Map<String,LinkedBlockQueue> named mapQueue. The key of mapQueue is source and symbol, data have the same source and symbol need to push the same key
    - Implement 2 methods enqueue and dequeue using behavior actor.
    - After ClusterBatchFetcherActor process finished. ClusterBatchFetcherActor send AlertConfig transformed from entity to UserAlertQueueActor based on Pekko Actor.
  - Create PriceQueueActor as a single actor. This Actor mange LinkedBlockQueue 
    - Implement 2 methods enqueue and dequeue using behavior actor.
    - Message comming, the consumerActor at PriceEventConsumer need to tell PriceQueueActor, to enqueue the message.
  - 

## Task 9. Matching Actor Enhancement
Create matching folder with new logic.
    - Create Coordinator Matching Actor followed the logic:
        - Every second, Coordinator actor will spawn one or many actor, named MatchingActor based on application.yaml config. This MatchingActor will dequeue a batch size message from  PriceQueue (batchsize need to configured at application.yaml) and poll the batch size from UserAlertQueueActor based on source and symbol match to item in PriceQueue.
          If the data in UserAlertQueueActor exceed the batch size(configured from application.yaml). Matching Actor will tell to coordinator that coordinator need to spawn extra Matching Actor. The new matching actor check UserAlertQueueActor queue size actor. if exceeded, create new once. This is a loop spawn until the UserAlertQueueActor queue less than the configured value. The new actor can be shared cross nodes in a cluster.
        - The condition of matching only support ABOVE AND BELOW.
        - After matchedAlerts. The matching actor need to tell AlertManagerActor updateAlert.
## Task 9.1 PriceQueueActor
Requirement: onDequeue should be returned the data
Action: You must adjust the method onDequeue and return to the sender.
      