# traffic-streaming-pipeline
Real-Time Traffic Events Streaming Pipeline

## Quickstart
To launch the entire pipeline (requires Docker + Python 3.12):

```bash
./run_pipeline.sh

---

## 🧠 What this project does


---
## 💼 Business Use Case


---
## 🏗️ Architecture Design
![Architecture Diagram](images/...png)

---
## 🛠️ Tools & Rationale

---
## 📦 How to Run

## ⚔️ Challenges and Solutions

- **Challenge:** FRED API returns series in different granularities (monthly, quarterly).  
  **Solution:** Normalized frequencies and added date parsing logic before transformations.

- **Challenge:** Initial CSV loads overwrote old runs.  
  **Solution:** Added idempotent upserts into SQLite to preserve history.

- **Challenge:** Some series had missing values.  
  **Solution:** Implemented null checks and default handling before loading.
