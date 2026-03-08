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