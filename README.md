# 🛍️ Kafka Streams - Retail Sales Analytics

This is a simple **Spring Boot + Apache Kafka Streams** application that simulates retail sales, processes purchase and product data, performs real-time join and windowed aggregations, and writes output to Kafka topics.

---

### Topics Used
- `products` – holds static product data (id, price)
- `purchases` – simulates customer purchases
- `joined-purchases` – output of the join between purchases and products
- `total-sales` – 2-minute windowed aggregation of total sales per product

## 🛠️ Getting Started

### Prerequisites
- Java 11+
- Apache Kafka running locally on `localhost:9092`
- Maven

### 1. Start Kafka (local)

# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka Broker
bin/kafka-server-start.sh config/server.properties

### 2. Create Topics
kafka-topics.sh --create --bootstrap-server localhost:9092 --topic products --partitions 1 --replication-factor 1
kafka-topics.sh --create --bootstrap-server localhost:9092 --topic purchases --partitions 1 --replication-factor 1
kafka-topics.sh --create --bootstrap-server localhost:9092 --topic joined-purchases --partitions 1 --replication-factor 1
kafka-topics.sh --create --bootstrap-server localhost:9092 --topic total-sales --partitions 1 --replication-factor 1

### 3. Build and Run
mvn clean spring-boot:run
