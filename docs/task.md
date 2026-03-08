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