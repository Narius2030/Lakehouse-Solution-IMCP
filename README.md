# References

- [General Architecture of Data Lake](#general-architecture-of-data-lake)
- [Real-time Monitoring & Scheduling](#real-time-monitoring-&-scheduling)
- [MLOps Cycle](#mlops-cycle)
- [FastAPI-based Microservice](#fastAPI-based-microservice)


## 📌 Tech Stack

| Technology  | Purpose |
|-------------|---------|
| **Spark Streaming** | Stream processing for ingesting |
| **Apache Kafka** | Streaming event and data |
| **Apache Airflow** | Workflows & scheduling tasks |
| **MinIO & MongoDB** | Data Storage and Catalog |
| **Trino** | Federated SQL queries & seamless integration with BI tools |
| **Superset** | BI tool for business analytics |

## 🖥️ Infrastructure

| Resource       | Specification |
|---------------|--------------|
| **VPS OS**    | Ubuntu 24.0.2 |
| **CPU**       | 4-core Intel Xeon |
| **GPU**       | ❌ No GPU |
| **RAM**       | 10GB |
| **Storage**   | 200GB SSD |
| **Networking** | 1Gbps Bandwidth |

# General Architecture of Data Lake

- Builded a Data Lake following Medallion architecture with `catalog layer` and `storage layer` for storing image and its metadata
- Streamed events from `file uploading` and `captured images` from mobile app (was sent by API) into raw storage area, so that it helps data more various for AI training
- Integrated NLP and Image processings in ETL pipeline to periodically normalize images and metadata

![image](https://github.com/user-attachments/assets/00a49f5d-af81-43ed-b105-cde56329c584)

Detail Functional Layers

![image](https://github.com/user-attachments/assets/da4c583d-70fd-440d-b2cc-9ab08cf92fd2)

Metadata Layer

![image](https://github.com/user-attachments/assets/ab8bc77a-6933-4d04-9e82-6330bd9ec50a)

# Real-time Monitoring & Scheduling

Monitoring Dashboard for Data Lake

![dashboard](https://github.com/user-attachments/assets/5972e554-78b7-478f-a123-188aeb8ae52d)

Schedule tasks on Airflow

![image](https://github.com/user-attachments/assets/d77acb2e-6420-4577-81a8-6496c7f2ea77)

# MLOps Cycle

![image](https://github.com/user-attachments/assets/36a027c6-1eaa-4344-b695-3fff5b9583c6)

# FastAPI-based Microservice

> More detail in this [Repo](https://github.com/Narius2030/FastAPI-Microservice-IMCP.git)

- **Query Data Service:** Develop an APIs to retrieve metadata and images which were normalized in Data Lake for automated incremental learning process.
- **Model Deploying Service:** Develop an APIs to deploy model run on vps, and obtain streaming captured image and metadata from mobile app to data lake for incremental learning.
- Utilize Nginx to route and load balance among API service containers for **_reducing the latency_** and **_avoiding overload_** on each service.

![image](https://github.com/user-attachments/assets/88946be6-513e-4758-9ebe-11c1573c4c62)



