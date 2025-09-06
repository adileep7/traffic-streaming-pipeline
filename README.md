# 🚦 Real-Time Traffic Streaming Pipeline with Azure Maps, Kafka (Redpanda), and Spark

This project ingests **live traffic incidents from Azure Maps** and processes them through a streaming data pipeline.  
It simulates how a transportation authority or logistics company might:
- Monitor real-time road conditions
- Aggregate incident data over time
- Build downstream analytics dashboards for congestion patterns
  
---
## Quickstart
To launch the entire pipeline (requires Docker + Python 3.12):

```bash
./run_pipeline.sh
```
---
## 🧠 What This Project Does
This project builds a **real-time streaming data pipeline** that ingests live traffic incident data from the **Azure Maps Traffic API** and processes it end-to-end. 

- A **Python producer** fetches raw incident events and streams them into **Redpanda (Kafka)**.  
- **Spark Structured Streaming with Delta Lake** ingests those events into the **Bronze layer** (raw).  
- The pipeline then cleans and enriches data in the **Silver layer** (standardized, partitioned).  
- Finally, it computes 10-minute aggregations in the **Gold layer**, ready for dashboards or analytics.  
- **Prefect orchestration** automates and coordinates the producer + Spark streaming jobs.  

The result is a running system that demonstrates how real-time data can be collected, transformed, and aggregated for decision-making.
---
## 💼 Business Use Case
Imagine a **city transportation authority** or a **logistics company** that needs to monitor live road conditions:

- Detect and track traffic incidents (collisions, closures, construction)
- Aggregate incidents into time windows to spot congestion trends
- Feed insights into **dashboards, alerting systems, or navigation apps**
- Support **route optimization** for delivery fleets or emergency response units

This pipeline is a simplified, but realistic, version of what such organizations would deploy to handle **real-time traffic intelligence at scale**.
---
## 📦 How to Run
1) Clone repo & create `.env`
   ```bash
   cp .env.example .env
   # edit .env and add your Azure Maps API key

2) Create & activate virtual environment
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

3) Start everything with one command
./run_pipeline.sh --hold

4) Data will land in Delta tables:
data/bronze/incidents
data/silver/incidents
data/gold/agg_incidents_10m
---
## 🏗️ Architecture Design
![Architecture Diagram](images/...png)

---
## 🛠️ Tools & Rationale
- **Azure Maps Traffic API** → real-world, streaming JSON source
- **Redpanda (Kafka-compatible)** → lightweight local Kafka broker for event streaming
- **Apache Spark + Delta Lake** → scalable streaming ETL with bronze/silver/gold medallion architecture
- **Prefect** → orchestrates producer + Spark jobs, manages lifecycle
- **Docker Compose** → spins up Redpanda + Console quickly
- **Python** → producer & orchestration glue
---
## ⚔️ Challenges and Solutions
- **Azure Maps API returned 400 errors** → fixed by correcting query params and bounding box format
- **Producer crashed due to missing dependency** → replaced `kafka-python` with `confluent-kafka` for stability
- **Kafka connection refused** → solved by aligning ports (9093) in both `docker-compose.yml` and app config
- **Spark structured streaming cleanup warnings** → mitigated by using Delta format with checkpointing
- **Prefect task serialization errors** → ignored harmless cache warnings; could disable cache for `stop_all_processes`

- **Challenge:** Some series had missing values.  
  **Solution:** Implemented null checks and default handling before loading.
