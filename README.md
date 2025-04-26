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

![image](https://github.com/user-attachments/assets/923a659b-0401-4c68-a28b-704d6db14098)

Detail Functional Layers

![image](https://github.com/user-attachments/assets/da4c583d-70fd-440d-b2cc-9ab08cf92fd2)

# Real-time Monitoring & Scheduling

Real-time Dashboard for Data Lake

![imcp-eda-2025-04-12T10-15-45 213Z](https://github.com/user-attachments/assets/3fafa081-69c4-4b59-85a4-1ee59f4cebc6)

Schedule tasks on Airflow

![image](https://github.com/user-attachments/assets/d77acb2e-6420-4577-81a8-6496c7f2ea77)

# MLOps Cycle

![image](https://github.com/user-attachments/assets/31effa68-39dc-4c92-860a-074c959b911a)

# FastAPI-based Microservice

> More detail in this [Repo](https://github.com/Narius2030/FastAPI-Microservice-IMCP.git)

- Develop an APIs to retrieve metadata and images which were normalized in Data Lake for automated incremental learning process.
- Develop an APIs to upload captured image and metadata of user to storage system for later usages and then activate model.
- Utilize Nginx to route and load balance among API service containers for **_reducing the latency_** and **_avoiding overload_** on each service.

![image](https://github.com/user-attachments/assets/11163700-dade-444e-8b19-d97bb7083237)


