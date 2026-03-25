# 📊 Log Aggregation & Monitoring System

A scalable backend system designed to **collect, process, and monitor logs in real-time** using modern distributed technologies like **Kafka, gRPC, and Elasticsearch**.

---

## 🚀 Features

* 📥 High-throughput log ingestion via **gRPC**
* 🧵 Distributed messaging using **Apache Kafka**
* ⚡ Concurrent log processing
* 📦 Batch indexing for performance optimization
* 🔍 Fast log querying using **Elasticsearch**
* 📊 Monitoring-ready architecture

---

## 🏗️ Architecture

![Architecture](./Architecture.png)

**Flow:**

* **gRPC Producer → Kafka Topic → Kafka Consumer → Elasticsearch**
* Metadata stored via **Hibernate (DB)**
* Logs indexed for fast search & monitoring

---

## 🛠️ Tech Stack

* Java (Spring Boot)
* Apache Kafka
* gRPC
* Elasticsearch
* Hibernate (JPA)

---

## 📂 Project Structure

```bash
log-aggregation-monitoring-system/
│
├── log-aggregation-monitoring-system-server/
│   ├── config/
│   ├── kafka/
│   ├── grpc/
│   ├── elasticsearch/
│   ├── model/
│   ├── repository/
│   └── proto/
│
├── application.yml
└── pom.xml
```

---

## ⚙️ Setup & Run

### 1. Start Dependencies

* Kafka (with Zookeeper)
* Elasticsearch

### 2. Run Application

```bash
mvn spring-boot:run
```

---

## 🧪 Test Cases (Step-by-Step Optimization)

---

### ✅ 1. Normal Processing Test (Baseline)

![Normal Test](./NormalTest.png)

**Description:**

* Logs are consumed one by one
* Each log is indexed immediately

**Result:**

* Total Logs: `500`
* Time Taken: `~20956 ms`

**Observation:**

* ❌ Very slow due to **individual Elasticsearch writes**
* ❌ High I/O overhead

---

### 📦 2. Batch Processing Test (Optimization Step 1)

![Batch Test](./BatchTest.png)

**Logic:**

```java
if(batch.size() >= 50){
    logIndexService.saveAll(batch);
    batch.clear();
}
```

**Description:**

* Logs are grouped into batches before indexing
* Reduces number of calls to Elasticsearch

**Result:**

* Total Logs: `500`
* Time Taken: `~1643 ms`

**Observation:**

* ✅ Massive improvement over normal processing
* ✅ Reduced I/O operations
* ⚖️ Balanced performance and resource usage

---

### ⚡ 3. Concurrent Processing Test (Optimization Step 2)

![Concurrent Test](./ConcurrentTest.png)

**Configuration:**

```java
factory.setConcurrency(3);
```

**Description:**

* Multiple Kafka consumers process logs in parallel
* Works best when combined with batching

**Result:**

* Total Logs: `500`
* Time Taken: `~759 ms`

**Observation:**

* 🚀 Fastest approach
* ✅ Parallel processing boosts throughput
* ✅ Ideal for production-scale systems

---

## 📊 Performance Comparison

| Step | Test Type  | Time Taken | Improvement            |
| ---- | ---------- | ---------- | ---------------------- |
| 1    | Normal     | ~20956 ms  | Baseline               |
| 2    | Batch      | ~1643 ms   | 🔥 Huge improvement    |
| 3    | Concurrent | ~759 ms    | 🚀 Maximum performance |

---

## 🔥 Key Learnings

* Start with **baseline (normal processing)**
* Add **batching → reduces I/O bottleneck**
* Add **concurrency → maximizes throughput**
* Best production setup:

  * ✅ Batch + Concurrency combined

---

## 📌 Future Improvements

* Add **Retry mechanism & Dead Letter Queue (DLQ)**
* Integrate **Kibana/Grafana dashboards**
* Implement **Backpressure handling**
* Add **Monitoring & alerting system**

---

## 👨‍💻 Author

Ritesh Bamola

---

## ⭐ Contribution

Feel free to fork, improve, and contribute 🚀
